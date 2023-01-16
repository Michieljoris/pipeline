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


;; (defn apply-xf-c
;;   "Default implementation. Calls the pipe's xf function on wrapped data and
;;    updates pipe to the next one."
;;   [{:keys [data pipe] :as x}]
;;   (let [r (merge x {:data ((:xf pipe) data)
;;                     :pipe (:next pipe)})]
;;     (tap> {:apply-xf-c x :result r})
;;     (a/to-chan! [r])))

;; (defn apply-xf
;;   "Default implementation. Calls the pipe's xf function on wrapped data and
;;    updates pipe to the next one."
;;   [{:keys [data pipe] :as x}]
;;   (merge x {:data ((:xf pipe) data)
;;             :pipe (:next pipe)}))

;; (defn enqueue
;;   "Default implementation. Enqueue x on the appropriate queue. Queueing should
;;    block in a go thread. check-in should be called before every queueing,
;;    check-out should be called after all results are queued"
;;   [{:keys [data pipe] :as x} queues]
;;   (let [{:keys [check-in check-out out]} (meta x)
;;         queue (get queues (:i pipe))]
;;     (a/go
;;       (if (queue? pipe data)
;;         (do  (check-in)
;;              (a/>! queue x))
;;         (a/>! out x))
;;       (check-out))))
