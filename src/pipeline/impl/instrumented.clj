(ns pipeline.impl.instrumented
  (:require [clojure.core.async :as a]
            [pipeline.stat :as stat]
            [pipeline.util :as u]))


(def mx-bean (java.lang.management.ManagementFactory/getThreadMXBean))

(defn wrapped [source pipeline]
  (let [pipeline-fn (if (fn? pipeline) pipeline (constantly pipeline))
        input (a/chan 1 (map #(hash-map :data % :pipeline (pipeline-fn %))))]
    (a/pipe source input)))

(defn wrap-apply-xf [apply-xf]
    (let [i (atom -1)]
      (fn [x]

        (stat/add-stat :queued (- (stat/now) (or (:result-queued x) (:queued x))))
       (let [{:keys [log-count log-period]
              :or {log-count u/noop
                   log-period u/noop} :as pipe} (first (:pipeline x))]
         (log-count)
         (log-period)
         (let [now (stat/now)
               result-channel (a/chan 1 (map #(assoc % :result-queued (stat/now))))
               c (apply-xf
                  (cond-> x
                    (not (:i x)) (assoc :i (swap! i inc))))]
           (stat/add-stat (keyword (str "xf-" (:i pipe))) (- (stat/now) now))
           (a/pipe c result-channel)
           result-channel
           ;; c
           )))))

(defn apply-xf
  "Actually calls the xf function on data and updates pipe to the next one.
   Handler functions passed to wrapper together with x"
  [{:keys [data pipeline] :as x}]
  (let [datas (try
                (let [{:keys [xf mult]} (first pipeline)
                      result (xf data)]
                  (if mult
                    (if (seq result) result [nil])
                    [result]))
                (catch Throwable t [t]))
        x' (assoc x :pipeline (rest pipeline))]
    (a/to-chan! (map #(assoc x' :data %) datas))))

(defn queue?
  "Decide on queueing for further processing."
  [{:keys [pipeline data]}]
  (and (seq pipeline) (some? data)
       (not (instance? Throwable data))))

(def task-ratios (atom []))

;; (add-watch task-ratios :k (fn [_ _ _ ratios]
;;                            (let [ratios (take-last 100 ratios)]
;;                              (when (pos? (count ratios))
;;                                (tap> {:task-ratio (/ (reduce + 0 ratios) (count ratios))})))
;;                          ))


(def os-bean (java.lang.management.ManagementFactory/getOperatingSystemMXBean))
(def cpu-time-a (atom {:last-time 0
                       :collect []
                       }))

(add-watch cpu-time-a :k (fn [_ _ _ {:keys [last-time collect total elapsed]}]

                           (when (and (number? elapsed) (zero? elapsed))
                             (tap>  {:system-cpu-load (.getSystemCpuLoad os-bean)
                                     :jvm-cpu-load (.getProcessCpuLoad os-bean)})
                             ;; (tap> total)
                             )
                         ;; (tap> {:cpu-time cpu-time})
                           ;; (let [cpu-time (take-last 100 cpu-time)]
                           ;;   (tap> {:cpu-time (/ (reduce + 0 cpu-time) 1000.0)}))
                         ))

(defn thread
  "Default implementation. Receives wrapped data, should call done when work is done, and
   return a channel with results."
  [apply-xf x done]
  (let [result (a/chan)]
    (a/thread
              (let [now (System/currentTimeMillis)
                    start-cpu-time (.getCurrentThreadCpuTime mx-bean)]
                (a/pipe (apply-xf x) result)
                ;; (test/rand-work 100 0)
                ;; (Thread/sleep 100)
                ;; (test/rand-work 500 0)

                ;; (let [cpu-time (Math/round (/(- (.getCurrentThreadCpuTime mx-bean) start-cpu-time)
                ;;                              (* 1000 1000.0)))

                ;;       time-spent (- (System/currentTimeMillis) now)
                ;;       task-time (- (System/currentTimeMillis) (:queued x))]
                ;;   ;; (swap! task-ratios conj (/ cpu-time time-spent 1.0))
                ;;   (swap! cpu-time-a (fn [{:keys [last-time collect]}]
                ;;                       (if (< last-time (- (System/currentTimeMillis) 1000))
                ;;                         {:last-time (System/currentTimeMillis)
                ;;                          :elapsed 0
                ;;                          :collect [cpu-time]
                ;;                          :total (reduce + 0 collect)
                ;;                          }
                ;;                         {:last-time last-time
                ;;                          :elapsed (- (System/currentTimeMillis) last-time )
                ;;                          :collect (conj collect cpu-time)}
                ;;                         )

                ;;                       ))
                ;;   ;; (tap> {:block-ratio (/ cpu-time time-spent 1.0)})
                ;;   ;; (tap> {:in-thread {;; :queued (- now (:queued x))
                ;;   ;;                    ;; :cpu-time          cpu-time
                ;;   ;;                    ;; :time-spent        time-spent
                ;;   ;;                    :block-ratio       (/ cpu-time time-spent 1.0)
                ;;   ;;                    ;; :task-time task-time
                ;;   ;;                    ;; :task-ratio (/ time-spent task-time 1.0)
                ;;   ;;                    ;; :current-thread-id (.getId (Thread/currentThread))
                ;;   ;;                    }})
                ;;   )
                )
              (done))
    result))
