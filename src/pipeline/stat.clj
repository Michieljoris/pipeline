(ns pipeline.stat
  (:require
   [clj-time.core :as t]
   [clj-time.coerce :as c]
   [pipeline.frequencies :as freq]
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
(defn now []
  (c/to-long (t/now)))

(defn mark [x event]
  (assoc x event (now)))

(defn update-stat-map [{:keys [bucket-size]
                        :or {bucket-size 1}
                        :as stat-map} val]
  (-> stat-map
      (update-in [:data (cond->> val
                          bucket-size (freq/bucket bucket-size))]
                 (fnil inc 0))
      (update :total (fnil inc 0))
      (update :period (fnil inc 0))))

(defn add-stat [data-point value]
  (swap! stats-atom update data-point #(update-stat-map % value)))

(defn get-duration-s [kw-or-number]
  (cond->> kw-or-number
    (keyword? kw-or-number) (get durations)))

(defn periodically [f period halt]
  (a/go-loop []
    (let [[_ c] (a/alts! [(a/timeout (* (get-duration-s period) 1000)) halt])]
         (when-not (= c halt)
           (f)
           (recur)))))

(comment
  (def halt (a/chan))
  (periodically #(tap> :hello) :one-second halt)
  (a/close! halt))

(defn mark-period [max-periods]
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

(defn init-stats [stat-maps minutes halt]
  (->> stat-maps
       (reduce (fn [acc {:keys [data-point bucket-size]}]
                 (assoc acc data-point {:data {}
                                        :init-time (now)
                                        :total 0
                                        :periods (list)
                                        :period 0
                                        :bucket-size bucket-size})) {})
       (reset! stats-atom))
  (let [max-periods (/ (* minutes 60) (get-duration-s default-period))]
    (periodically (partial mark-period max-periods) default-period halt)))

(defn stop-stats [stop]
  (reset! stop true))

(defn bucket-stats [bucket-map bucket-size & args]
  (apply freq/stats (freq/recover-bucket-keys bucket-map bucket-size) args))

(defn stats [data-point & args]
  (let [{:keys [data bucket-size periods]
         :or {bucket-size 1}}
        (get @stats-atom data-point)
        freq-stats (apply freq/stats (cond-> data
                                       bucket-size (freq/recover-bucket-keys bucket-size)) args)]
    (-> freq-stats
        (assoc :rate-per-second {:last-10-seconds (rate periods {:range :ten-seconds})
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

;; (defn reset-stats []
;;   (swap! stats-atom (fn [stat-maps]
;;                       (reduce-kv (fn [acc data-point {:keys [bucket-size]}]
;;                                    (assoc acc data-point {:data {}
;;                                                           :bucket-size {}}))
;;                                  {}
;;                                  stat-maps))))


;; pipeline stat fns
(def thread-details (atom []))

(defn xf-stats [{:keys [xfs xf-start-time submit-time queued-time] :as x}]
  (let [now-ms      (now)
        step        (-> xfs first :step)
        xf-i        (keyword (str "xf-" step))
        xf-duration (- now-ms xf-start-time)]
    (add-stat :wait (- xf-start-time submit-time))
    (add-stat :queued (- xf-start-time queued-time))
    (add-stat :xf xf-duration)
    (add-stat xf-i xf-duration))
  x)




;; more



(def halt (atom true))
(comment
  ;; (periodically mark-period default-period)
  )

(defn update-freq-map [freq-map val]
  (update freq-map val (fnil inc 0)))

(defn update-bucket-map [bucket-map bucket-size val]
  (update bucket-map (freq/bucket bucket-size val) (fnil inc 0)))

;; (-> (update-bucket-map {} 10 1)
;;      (update-bucket-map 1 10)
;;      )



;; (freq/stats (freq/bucket-frequencies 10 [1 4 15 20]))
;; (freq/bucket-frequencies 10 [1 4 15 20])



;; (-> (update-bucket-map {} 10 1)
;;     (update-bucket-map 4 10)
;;     (update-bucket-map 15 10)
;;     (update-bucket-map 20 10)
;;     (bucket-stats 10)
;;     )


;; (add-watch stats-atom :stats (fn [_ _ old new]
;;                           (tap> {:stats-watch {:old old :new new}})
;;                           ))



;; (deref stats-atom)


;; (add-stat (keyword (str (name data-point) "-rate-per-second"))
;;           period)




(comment
  ;; (mark-period)

  ;; (init-stats [{:data-point :foo
  ;;               :bucket-size 10}
  ;;              {:data-point :bar
  ;;               :bucket-size 10}])
  (:foo1 @stats-atom)

  (reset! halt true)
  (do
    (reset! halt nil)
    (periodically mark-period default-period halt))
  (add-stat :foo1 1)
  (add-stat :foo1 4)
  (add-stat :foo1 15)
  (add-stat :foo1 20)
  ;; (reset-stats)
  (stats 3))



(defn throw-err [e]
  (when (instance? Throwable e) (throw e))
  e)

(defmacro <? [ch]
  `(throw-err (a/<! ~ch)))

;; (defn submit*
;;   [pool f]
;;   (let [result (.submit ^ExecutorService pool ^Callable f)]
;;     result))

;; (defmacro submit
;;   [pool & body]
;;   `(submit* ~pool (fn* [] ~@body)))

(comment
  (a/go
    (try
      (<? (a/thread
            (try
              (throw (ex-info "foo" {}))
              (catch Throwable t
                t)
              )
            ))
      (catch Throwable e
        (tap> {:e!!! e}))
      )))


(defn record-thread-details [thread-details {:keys [index xfs]} thread on?]
  (swap! thread-details conj {:thread thread
                              :index index
                              :step (-> xfs first :step)
                              :timestamp (now)
                              :on? on?}))


(def last-timestamp (atom nil))
(def max-thread-count (atom 10))
(do
  (reset! last-timestamp nil)
  (defn thread-activity [xfs]
    (let [thread-count @max-thread-count
          thread-vector (into [] (take thread-count (repeat
                                                     ;; :xxxx
                                                     nil
                                                            )))]
      (->> @thread-details
           (reduce (fn [acc {:keys [thread xfs-i index on? timestamp]}]
                     (when (and @last-timestamp on?)

                     (tap> (- timestamp @last-timestamp))
                       (add-stat :thread-on (- timestamp @last-timestamp))
                       )
                     (reset! last-timestamp timestamp)
                     (let [last-thread-vector (last acc)]
                       (conj acc (assoc last-thread-vector thread (if on?
                                                                      [index (inc xfs-i)]
                                                                      []
                                                                      ;; :xxxx
                                                                      )))))
                   [thread-vector])
           (map (fn [v]
                  (let [v' (into [] (sort (keep second v)))
                        thread-count (count v')
                        freq-map (frequencies v')]
                    (->> (range (count xfs))
                         (reduce (fn [acc xfs-i]
                                   (conj acc
                                         (if-let [freq (get freq-map (inc xfs-i))]
                                           (int (* 100.0 (/ freq thread-count)))
                                           0
                                           ))
                                   )
                                 [(str thread-count)]
                                 ))
                    )

                  ))
           ;; (map #(sort (fn [x y]
           ;;               (cond
           ;;                 (and (vector? x) (keyword? y)) -1
           ;;                 (and (vector? y) (keyword? x)) 1
           ;;                 (and (vector? x) (vector? y)) (compare (first x) (first y))
           ;;                 :else 1)) %))
           ;; (map str)
           )
      ))
  ;; (thread-activity)
  )

;; (defn process-source [{:keys [source xfs]} monitor
;;                       {:keys [cpu-thread-count io-thread-count log complete abort?] :as opts}]
;;   (let [xfs-for-source (map-indexed #(assoc %2 :step %1) xfs)
;;         max-xfs-count (count xfs-for-source)
;;         queues   (->> (repeatedly #(vector (chan) (chan))) (take max-xfs-count) (into [])) ;;NOTE: minimum nr of queues is (max  (map count <all of xfs used>))
;;         source-x (chan 1 (map-indexed (fn [index data]
;;                                         {:data  data
;;                                          :batch 1
;;                                          :xfs   (map #(assoc %1 :queue (if (:blocking %1)
;;                                                                          (first %1) (second %2))) xfs-for-source queues) ;; NOTE: every x could have a different xfs to apply
;;                                          :index index :begin (stat/now)})))
;;         [io-queues cpu-queues] (reduce (fn [[io-queues cpu-queues] [io-queue cpu-queue]]
;;                                          [(conj io-queues io-queue) (conj cpu-queues cpu-queue)])
;;                                        [[] []] queues)]

;;     (a/onto-chan! source-x source)
;;     (process-queues cpu-queues cpu-thread-count monitor opts)
;;     ;; (process-queues cpu-queues io-thread-count monitor opts)

;;     (go-loop [{:keys [xfs batch] :as x} (a/<! source-x)]
;;       (if (and (nil? x) batch)
;;         (watch-monitor monitor batch)
;;         (let [xf (first xfs)
;;               x' (assoc x :status (get-status xf abort? x))]
;;           (log {:submitting x})
;;           (if (= :to-queue (:status x'))
;;             (submit (:queue xf) x')
;;             (when complete (s-put! complete x')))
;;           (recur (a/<! source-x)))))

;;     (let [[io-queue cpu-queue] (get queues (dec (count xfs)))]
;;          (if (:blocking (last xfs)) io-queue cpu-queue))
;;     ))