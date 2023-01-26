(ns pipeline.impl.instrumented
  (:require [clojure.core.async :as a]
            [pipeline.impl.wrapped :as w]
            [pipeline.stat :as stat]
            [pipeline.util :as u]))

(def mx-bean (java.lang.management.ManagementFactory/getThreadMXBean))
(def os-bean (java.lang.management.ManagementFactory/getOperatingSystemMXBean))

(defn wrapped [source pipeline]
  (let [pipeline-fn (if (fn? pipeline) pipeline (constantly pipeline))
        input (a/chan 1 (map-indexed #(hash-map :data %2
                                                :pipeline (pipeline-fn %2)
                                                :i %1
                                                :ts-source-queued (stat/now))))]
    (a/pipe source input)))

(defn apply-xf
  ([x] (apply-xf hash x))
  ([pipe-id-fn x]
   (stat/add-stat :wait (- (stat/now) (or (:result-queued x) (:queued x))))
   (let [{:keys [log-count log-period]
          :or {log-count u/noop
               log-period u/noop} :as pipe} (first (:pipeline x))]
     (log-count)
     (log-period)
     (let [now (stat/now)
           result-channel (a/chan 1 (map #(assoc % :result-queued (stat/now))))
           c (w/apply-xf x)]
       (stat/add-stat (keyword (str "xf-" (pipe-id-fn pipe))) (- (stat/now) now))
       (stat/add-stat (keyword (str "xf")) (- (stat/now) now))
       (a/pipe c result-channel)
       result-channel))))

(def queue? w/queue?)

(def thread w/wrapped)

(defn out []
  (a/chan 1 (map (fn [x]
                   (stat/add-stat :in-system (- (stat/now) (:ts-source-queued x))) x))))