(ns pipeline.core
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as s]
            [pipeline.util :as u]))

(defn- update-x
  "Actually calls the xf function on data and updates pipe to the next one."
  [{:keys [data pipe] {:keys [xf mult]} :pipe :as x}]
  (assoc x :data (try
                   (let [result (xf data)]
                     (if mult
                       (if (seq result) result [nil])
                       [result]))
                   (catch Throwable t [t]))
         :pipe (:next pipe)))

(defn default-wrapper
  "Expects the update-x fn to be called on x and the result to be returned. To be
   used to hook into pre and post (xf data), eg. for stats or debugging."
  [update-x x]
  (update-x x))

(defn- process-x
  "Applies the pipe as set on x to data as set on x, then queues the results."
  [{:keys [out check-in check-out] :as x} wrapper queues]
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
          (check-out))))

(defn threads
  "Starts up thread-count threads, and creates queue-count queue channels. Each
   thread is set up to process data as put on the queues. Returns the first of
   the queues and a halt channel, that when closed stops all threads. The
   thread-hook function gets called with the thread number every time the
   thread starts any new work."
  [thread-count queue-count {:keys [thread-hook wrapper halt]
                             :or {thread-hook u/noop wrapper default-wrapper halt (a/chan)}}]
  (u/assert-spec ::threads-args {:thread-count thread-count :queue-count queue-count
                                 :thread-hook thread-hook})
  (let [queues (->> (repeatedly a/chan) (take queue-count) vec)
        p-queues (reverse (into [halt] queues))]
    (dotimes [thread-i thread-count]
      (a/thread
        (loop []
          (thread-hook thread-i)
          (let [[x _] (a/alts!! p-queues :priority true)]
            (when x ;;highest priority halt can be closed->thread finishes
              (process-x x wrapper queues)
              (recur))))))
    {:queue (first queues) :halt halt}))

(defn as-pipe
  "Prepares xfs (list of maps, each having at least a single argument function
   under the :xf key) so that it can be passed to the flow function."
  ([xfs] (as-pipe xfs 0))
  ([xfs offset]
   (->> xfs (map-indexed #(assoc %2 :i (+ offset %1))) u/linked-list)))

(defn flow
  "Takes elements from the in channel and pipes them to the out channel, using
   worker, producing 1 or more outputs per input. The pipe gets assoced with
   every input element and is used by the threads to apply the right
   transformation. Results are unordered relative to input. By default, the to
   channel will be closed when the from channel closes, but can be determined by
   the close? parameter. Will stop consuming the in channel if the out channel
   closes."
  ([in pipe worker] (flow in pipe worker (a/chan) nil))
  ([in pipe worker out {:keys [close?] :or {close? true}}]
   ;; (u/assert-spec ::flow-args {:source in :pipeline-xfs pipe})
   (let [monitor (atom 0)
         check-in #(swap! monitor inc)
         check-out #(when (zero? (swap! monitor dec))
                      (when close? (a/close! out)))]
     (check-in)
     (a/go
       (loop []
         (if-let [data (a/<! in)]
           (let [x {:check-in check-in :check-out check-out :out out
                    :data data  :pipe pipe }]
             (check-in)
             (when (a/>! worker x) (recur)))
           (check-out))))
     out)))


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