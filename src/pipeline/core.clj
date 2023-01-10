(ns pipeline.core
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as s]
            [pipeline.util :as u]))

(defn apply-xf
  "Calls the pipe's xf function on data and updates pipe to the next one."
  [{:keys [data pipe] :as x}]
  (merge x {:data ((:xf pipe) data)
            :pipe (:next pipe)}))

(defn queue?
  "Decide on queuing for further processing."
  [pipe data]
  (not (or (empty? pipe)
           (nil? data))))

(defn enqueue
  "Enqueue x on the appropriate queue. Queueing should block in a go thread.
   check-in should be called before every queueing, check-out should be called
   after all results are queued"
  [{:keys [data pipe] :as x} queues]
  (let [{:keys [check-in check-out out]} (meta x)
        queue (get queues (:i pipe))]
    (a/go
      (if (queue? pipe data)
        (do  (check-in)
             (a/>! queue x))
        (a/>! out x))
      (check-out))))

(defn worker
  "Starts up thread-count threads, and creates queue-count queue channels. Each
   thread is set up to process data as put on the queues. Returns the first of
   the queues. Halt when closed stops all threads. The thread-hook function gets
   called with the thread number every time the thread starts any new work."
  ([thread-count] (worker thread-count nil))
  ([thread-count {:keys [queue-count thread-hook halt apply-xf enqueue]
                  :or   {thread-hook u/noop halt (a/chan) queue-count 100
                         enqueue enqueue
                         apply-xf apply-xf}}]
   (let [queues (->> (repeatedly a/chan) (take queue-count) vec)
         p-queues (reverse (into [halt] queues))]
     (dotimes [thread-i thread-count]
       (a/thread
         (loop []
           (thread-hook thread-i)
           (let [[x _] (a/alts!! p-queues :priority true)]
             (when x
               (enqueue (apply-xf x) queues)
               (recur))))))
     (first queues))))

(defn as-pipe
  "Prepares xfs (list of maps, each having at least a single argument function
   under the :xf key) so that it can be passed to the flow function."
  ([xfs] (as-pipe xfs 0))
  ([xfs offset]
   (->> xfs (map-indexed #(assoc %2 :i (+ offset %1))) u/linked-list)))

(defn flow
   "Flows in through the pipe using worker, producing 1 or more outputs per input.
   Results are put on the returned out channel, which can be passed. The pipe
   gets assoced with every input element and is used by the threads to apply the
   right transformation. Results are unordered relative to input. By default,
   the to channel will be closed when the from channel closes, but can be
   determined by the close? parameter. Will stop consuming the in channel if the
   out channel closes."
  ([in pipe worker] (flow in pipe worker nil))
  ([in pipe worker {:keys [out close?] :or {close? true out (a/chan)}}]
   (let [monitor (atom 0)
         check-in #(swap! monitor inc)
         check-out #(when (and (zero? (swap! monitor dec)) close?)
                      (a/close! out))
         wrapped-x (with-meta {} {:check-in check-in :check-out check-out :out out})
         pipe-fn (if (fn? pipe) pipe (constantly pipe))]
     (check-in)
     (a/go
       (loop []
         (when-let [data (a/<! in)]
           (check-in)
           (when (a/>! worker (assoc wrapped-x :data data :pipe (pipe-fn data)) )
             (recur))))
       (check-out))
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