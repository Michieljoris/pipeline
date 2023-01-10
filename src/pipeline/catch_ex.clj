(ns pipeline.catch-ex
  (:require [clojure.core.async :as a]))

(defn apply-xf
  "Calls the xf function on data and updates pipe to the next one."
  [{:keys [data pipe] :as x}]
  (merge x {:data (try ((:xf pipe) data) (catch Throwable t t))
            :pipe (:next pipe)}))

(defn queue? [pipe data]
  (not (or (instance? Throwable data)
           (empty? pipe)
           (nil? data))))

(defn enqueue
  "Enqueue x on the appropriate queue."
  [{:keys [data pipe] :as x} queues]
  (let [{:keys [check-in check-out out]} (meta x)
        queue (get queues (:i pipe))]
    (a/go
      (if (queue? pipe data)
        (do  (check-in)
             (a/>! queue x))
        (a/>! out x))
      (check-out))))