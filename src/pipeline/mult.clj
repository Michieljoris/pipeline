(ns pipeline.mult
  (:require [clojure.core.async :as a]))

(defn update-x
  "Actually calls the xf function on data and updates pipe to the next one.
   Handler functions passed to wrapper together with x"
  [{:keys [data pipe] {:keys [xf mult]} :pipe :as x}]
  (merge x {:data (try
                    (let [result (xf data)]
                      (if mult
                        (if (seq result) result [nil])
                        [result]))
                    (catch Throwable t [t]))
            :pipe (:next pipe)}))

(defn queue? [pipe data]
  (not (or (instance? Throwable data)
           (empty? pipe)
           (nil? data))))

(defn enqueue
  [{:keys [data pipe] :as updated-x} queue]
  (let [{:keys [check-in check-out out]} (meta updated-x)]
    (a/go
      (doseq [data data]
        (let [x-to-queue (assoc updated-x :data data)]
          (if (queue? pipe data)
            (do
              (check-in)
              (a/>! queue x-to-queue))
            (a/>! out x-to-queue))))
      (check-out))))