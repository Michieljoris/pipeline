(ns pipeline.core
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as s]
            [pipeline.util :as u]))

(defn apply-pipe [{:keys [f mult]} data]
  (try
    (let [result (f data)]
      (if mult
        (if (seq result) result [nil])
        [result]))
    (catch Throwable t [t])))

(defn- update-x [{:keys [pipe data] :as x}]
  (assoc x :data (apply-pipe pipe data)))

(defn default-wrapper [update-x {:keys [pipe] :as x}]
  (assoc (update-x x) :pipe (:next pipe)))

(defn threads
  "Starts up thread-count threads, and creates queue-count queue channels. Each
   thread is set up to process data as put on the queues. Returns a map with the
   queues and a halt channel, that when closed stops all threads. The
   process-hook function gets called with the thread number every time the
   thread starts any work."
  [thread-count queue-count {:keys [process-hook wrapper]
                             :or {process-hook u/noop
                                  wrapper default-wrapper}}]
  (u/assert-spec ::threads-args {:thread-count thread-count :queue-count queue-count :process-hook process-hook})
  (let [halt (a/chan)
        queues (->> (repeatedly a/chan) (take queue-count) vec)
        p-queues (reverse (into [halt] queues))]
    (dotimes [thread-i thread-count]
      (a/thread
        (loop []
          (process-hook thread-i)
          (let [[{:keys [out check-in check-out] :as x} _] (a/alts!! p-queues :priority true)]
            (when x ;;highest priority halt can be closed->thread finishes
              (let [{:keys [data pipe] :as updated-x} (wrapper update-x x)
                    queue (get queues (:i pipe))]
                (a/go (doseq [data data]
                        (let [status (cond (instance? Throwable data) :error
                                           (empty? pipe)              :result
                                           (nil? data)                :nil
                                           :else                      :queue)
                              x-to-queue (merge updated-x
                                                {:out out :check-in check-in :check-out check-out
                                                 :data data :status status})]
                          (if (= status :queue)
                            (do
                              (check-in)
                              (a/>! queue x-to-queue))
                            (a/>! out x-to-queue))))
                      (check-out)))
              (recur))))))
    {:queue (first queues) :halt halt}))

(defn as-pipe
  ([xfs] (as-pipe xfs 0))
  ([xfs offset]
   (->> xfs (map-indexed #(assoc %2 :i (+ offset %1))) u/linked-list)))

(defn flow
  "Takes elements from the in channel and supplies them to the out channel,
   producing 1 or more outputs per input. The pipe gets assoced with every input
   element and is used by the threads to apply the right transformation. Results
   are unordered relative to input. By default, the to channel will be closed
   when the from channel closes, but can be determined by the close? parameter.
   Will stop consuming the in channel if the out channel closes."
  [in pipe out worker {:keys [close?] :or {close? true}}]
  ;; (u/assert-spec ::flow-args {:source in :pipeline-xfs pipe})
  (let [monitor (atom 0)
        check-in #(swap! monitor inc)
        check-out #(when (zero? (swap! monitor dec))
                   (when close? (a/close! out)))]
    (check-in)
    (a/go
      (loop []
        (if-let [data (a/<! in)]
          (let [x {:data data :check-in check-in :check-out check-out :out out :pipe pipe}]
            (check-in)
            (when (a/>! worker x) (recur)))
          (check-out))))
    out))


(s/def ::f fn?)
(s/def ::wrapped-f fn?)
(s/def ::mult (s/nilable boolean))
(s/def ::thread-count pos-int?)
(s/def ::queue-count pos-int?)
(s/def ::chan #(instance? clojure.core.async.impl.channels.ManyToManyChannel %))
(s/def ::halt ::chan)
(s/def ::queue ::chan)
(s/def ::queues (s/coll-of ::queue))
(s/def ::xf (s/keys :req-un [::f]
                    :opt-un [::mult]))
(s/def ::xfs (s/and (s/coll-of ::xf) seq))
(s/def ::pipeline-xf (s/keys :req-un [::wrapped-f ::queue]
                             :opt-un [::mult]))
(s/def ::pipeline-xfs (s/and (s/coll-of ::pipeline-xf) seq))
(s/def ::threads-args (s/keys :req-un [::thread-count ::queue-count]
                              :opt-un [::process-hook])) ;;TODO

(s/def ::pipeline-args (s/keys :req-un [::xfs ::queues]
                               :opt-un [::hooks])) ;;TODO

(s/def ::source (s/or :buffered-reader u/buffered-reader?  :coll coll? :channel u/channel? :fn fn?))

(s/def ::flow-args (s/keys :req-un [::source ::pipeline-xfs]))