(ns pipeline.impl.instrumented
  (:require [clojure.core.async :as a]
            [pipeline.impl.default :as d]
            [pipeline.stat :as stat]
            [pipeline.util :as u]))

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
   (stat/add-stat :wait (- (stat/now) (or (:ts-result-queued x) (:ts-source-queued x))))
   (let [{:keys [log-count log-period]
          :or {log-count u/noop
               log-period u/noop} :as pipe} (first (:pipeline x))]
     (log-count)
     (log-period)
     (let [ts-before-apply-xf (stat/now)
           result-channel (a/chan 1 (map #(assoc % :ts-result-queued (stat/now))))
           c (d/apply-xf x)
           duration (- (stat/now) ts-before-apply-xf)]
       (stat/add-stat (keyword (str "xf-" (pipe-id-fn pipe))) duration)
       (stat/add-stat (keyword (str "xf")) duration)
       (a/pipe c result-channel)
       result-channel))))

(defn out []
  (a/chan 1 (map (fn [x]
                   (stat/add-stat :in-system (- (stat/now) (:ts-source-queued x)))
                   x))))