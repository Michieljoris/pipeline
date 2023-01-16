(ns pipeline.mult
  (:require [clojure.core.async :as a]
            [pipeline.catch-ex :as catch-ex]))

(defn apply-xf
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

(defn apply-xf-c
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

(defn enqueue
  [{:keys [data pipe] :as x} queues]
  (let [{:keys [check-in check-out out]} (meta x)
        queue (get queues (:i pipe))]
    (a/go
      (doseq [data data]
        (let [x-to-queue (assoc x :data data)]
          (if (catch-ex/queue? pipe data)
            (do
              (check-in)
              (a/>! queue x-to-queue))
            (a/>! out x-to-queue))))
      (check-out))))