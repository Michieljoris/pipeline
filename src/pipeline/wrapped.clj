(ns pipeline.wrapped
  (:require [clojure.core.async :as a]))


(def mx-bean (java.lang.management.ManagementFactory/getThreadMXBean))

(defn wrapped [source pipeline]
  (let [pipeline-fn (if (fn? pipeline) pipeline (constantly pipeline))
        input (a/chan 1 (map #(hash-map :data % :pipeline (pipeline-fn %))))]
    (a/pipe source input)))

(defn apply-xf
  "Actually calls the xf function on data and updates pipe to the next one.
   Handler functions passed to wrapper together with x"
  [{:keys [data pipeline] :as x} result]
  (let [datas (try
                (let [{:keys [xf mult]} (first pipeline)
                      result (xf data)]
                  (if mult
                    (if (seq result) result [nil])
                    [result]))
                (catch Throwable t [t]))
        x' (assoc x :pipeline (rest pipeline))]
    (a/onto-chan! result (map #(assoc x' :data %) datas))))

(defn queue?
  "Decide on queueing for further processing."
  [{:keys [pipeline data]}]
  (and (seq pipeline) (some? data)
       (not (instance? Throwable data))))

(defn thread
  "Default implementation. Receives wrapped data, should call done when work is done, and
   return a channel with results."
  [x done]
  (let [result (a/chan)]
    (a/thread (apply-xf x result) (done))
    result))
