(ns pipeline.mult
  (:require [clojure.core.async :as a]))

(defn apply-xf
  "Actually calls the xf function on data and updates pipe to the next one.
   Handler functions passed to wrapper together with x"
  [{:keys [data pipe] {:keys [xf mult]} :pipe :as x} result-chan]
  (let [result (try
                 (let [result (xf data)]
                   (if mult
                     (if (seq result) result [nil])
                     [result]))
                 (catch Throwable t [t]))
        x' (assoc x :pipe (:next pipe))]
    (a/onto-chan! result-chan (map #(assoc x' :data %) result))))