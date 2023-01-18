(ns pipeline.mult
  (:require [clojure.core.async :as a]))

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