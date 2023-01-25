(ns pipeline.stat
  (:require
   [clj-time.core :as t]
   [clj-time.coerce :as c]
   [com.stuartsierra.frequencies :as freq]
   [clojure.core.async :as a]
   [taoensso.timbre :as log]))

(def stats-atom (atom nil))

(def default-period :one-second)

(def durations
    {:one-second 1
     :two-seconds 2
     :five-seconds 5
     :ten-seconds 10
     :one-minute 60
     :two-minutes (* 2 60)
     :five-minutes (* 5 60)
     :fifteen-minutes (* 15 60)
     :all (Integer/MAX_VALUE)})

;; stat fns
(defn now
  "Convenience function that returns now in milliseconds"
  []
  (c/to-long (t/now)))

(defn mark
  "Convenience function that assoces now in milliseconds to x under the event key"
  [x event]
  (assoc x event (now)))

(defn update-stat-map
  [{:keys [bucket-size]
    :or {bucket-size 1}
    :as stat-map} val]
  (-> stat-map
      (update-in [:data (cond->> val
                          bucket-size (freq/bucket bucket-size))]
                 (fnil inc 0))
      (update :total (fnil inc 0))
      (update :period (fnil inc 0))))

(defn add-stat
  "Add a value to data point"
  [data-point value]
  (swap! stats-atom update data-point #(update-stat-map % value)))

(defn get-duration-s [kw-or-number]
  (cond->> kw-or-number
    (keyword? kw-or-number) (get durations)))

(defn periodically
  "Executes f every period (in minutes) till halt channel is closed"
  [f period halt]
  (a/go-loop []
    (let [[_ c] (a/alts! [(a/timeout (* (get-duration-s period) 1000)) halt])]
      (when-not (= c halt)
        (f)
        (recur)))))

(defn mark-period
  [max-periods]
  (swap! stats-atom (fn [stat-maps]
                      (reduce (fn [stat-maps data-point]
                                (update stat-maps data-point
                                        (fn [{:keys [period] :as stat-map}]
                                          (-> stat-map
                                              (assoc :period 0)
                                              (update :periods (fnil conj (list)) period)
                                              (update :periods #(take max-periods %))))))
                              stat-maps
                              (keys stat-maps)))))

(defn round [f n]
  (when f
    (let [decimals (Math/pow 10 n)]
      (/ (Math/round (* f decimals)) decimals))))

(defn rate
  [series {:keys [range unit series-period]
           :as args
           :or {unit :one-second
                series-period default-period}}]
  (try
    (let [range-s (get-duration-s range)
          series-period-s (get-duration-s series-period)
          n (cond-> (int (/ range-s series-period-s))
              (= range :all) (min (count series)))]
      (when (> (count series) n)
        (let [total-hits (reduce + 0 (take n series))
              total-duration-s (* n series-period-s)
              rate-per-second (/ total-hits total-duration-s)
              unit-s (get-duration-s unit)]
          (round (float (* rate-per-second unit-s)) 2))))
    (catch Exception e
      (log/warn e "Error calculating rate for" args)
      {:error args} )))

(defn init-stats
  "Reset stats atom, and periodically updates it, keeping stats for up to minutes
   minutes. Returns channel that when closed will stop stat taking"
  [minutes]
  ;; (->> stat-maps
  ;;      (reduce (fn [acc {:keys [data-point bucket-size]}]
  ;;                (assoc acc data-point {:data {}
  ;;                                       :init-time (now)
  ;;                                       :total 0
  ;;                                       :periods (list)
  ;;                                       :period 0
  ;;                                       :bucket-size bucket-size})) {})
  ;;      (reset! stats-atom))
  (reset! stats-atom nil)
  (let [max-periods (/ (* minutes 60) (get-duration-s default-period))
        halt (a/chan)]
    (periodically (partial mark-period max-periods) default-period halt)
    halt))

(defn stats
  "Returns stats for data-point"
  [data-point & args]
  (let [{:keys [data bucket-size periods]
         :or {bucket-size 1}}
        (get @stats-atom data-point)
        freq-stats (apply freq/stats (cond-> data
                                       bucket-size (freq/recover-bucket-keys bucket-size)) args)]
    (-> freq-stats
        (assoc :rate-per-second {:last-second (rate periods {:range :one-second})
                                 :last-10-seconds (rate periods {:range :ten-seconds})
                                 :last-one-minute (rate periods {:range :one-minute})
                                 :last-five-minutes (rate periods {:range :five-minutes})
                                 :last-fifteen-minutes (rate periods {:range :fifteen-minutes})
                                 :all (rate periods {:range :all})})
        (assoc :rate-per-minute {:last-10-seconds (rate periods {:range :ten-seconds
                                                                 :unit :one-minute})
                                 :last-one-minute (rate periods {:range :one-minute
                                                                 :unit :one-minute})
                                 :last-five-minutes (rate periods {:range :five-minutes
                                                                   :unit :one-minute})
                                 :last-fifteen-minutes (rate periods {:range :fifteen-minutes
                                                                      :unit :one-minute})
                                 :all (rate periods {:range :all
                                                     :unit :one-minute})}))))
