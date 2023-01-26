(ns pipeline.impl.wrapped
  "Runs tasks in threads, when :mult flag is set to true on xf expects multiple
   results. Catches any errors and assigns them to :data."
  (:require [clojure.core.async :as a]))



(defn wrapped
  "Expects a channel as source, wraps elements in a map each bundled with
   pipeline, when pipeline is a function it'll be called on every source element
   and should return a pipeline (list of maps each with a transforming function
   under the :xf key)."
  [source pipeline]
  (let [pipeline-fn (if (fn? pipeline) pipeline (constantly pipeline))
        input (a/chan 1 (map #(hash-map :data % :pipeline (pipeline-fn %))))]
    (a/pipe source input)))

(defn apply-xf
  "Actually calls the xf function on data and updates pipe to the next one.
   Returns channel with (possilbe) multiple results. Catches any errors and
   assigns them to the :data key."
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
  "Decide on queueing for further processing. "
  [{:keys [pipeline data]}]
  (and (seq pipeline) (some? data)
       (not (instance? Throwable data))))

(defn thread
  "Receives wrapped data as x, should call apply-xf on x and then done, and
   return a channel with results."
  [apply-xf x done]
  (let [result (a/chan)]
    (a/thread (a/pipe (apply-xf x) result) (done))
    result))
