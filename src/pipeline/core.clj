(ns pipeline.core
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [pipeline.util :as u]))

(defn apply-xf
  "Default implementation. Calls the pipe's xf function on wrapped data and
   updates pipe to the next one."
  [{:keys [data pipe] :as x}]
  (merge x {:data ((:xf pipe) data)
            :pipe (:next pipe)}))

(defn queue?
  "Default implementation. Decide on queuing for further processing."
  [pipe data]
  (not (or (empty? pipe)
           (nil? data))))

(defn enqueue
  "Default implementation. Enqueue x on the appropriate queue. Queueing should
   block in a go thread. check-in should be called before every queueing,
   check-out should be called after all results are queued"
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
   the queues. Halt when closed stops all threads. The hook function gets
   called with the thread number every time the thread starts any new work."
  ([thread-count] (worker thread-count nil))
  ([thread-count {:keys [queue-count hook halt apply-xf enqueue]
                  :or   {hook u/noop halt (a/chan) queue-count 100
                         enqueue enqueue
                         apply-xf apply-xf}}]
   (let [queues (->> (repeatedly a/chan) (take queue-count) vec)
         p-queues (reverse (into [halt] queues))]
     (dotimes [thread-i thread-count]
       (a/thread
         (loop []
           (hook thread-i)
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
   Results are put on the returned out channel, which can be passed in. The
   pipe (or the result of calling pipe on source if pipe is a function) gets
   assoced with every input element and is used by the threads to apply the
   right transformation. Results are unordered relative to input. By default,
   the out channel will be closed when the in channel closes (once all
   processing is done), but this can be determined by the close? parameter.
   Consumes from the in channel as long as data is taken from the out channel or
   until out channel is closed (once all processing is done)."
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

;;TODO: finish specs
(s/def ::xf fn?)
(s/def ::apply-xf fn?)
(s/def ::enqueue fn?)
(s/def ::hook  fn?)
(s/def ::thread-count pos-int?)
(s/def ::queue-count pos-int?)
(s/def ::chan #(instance? clojure.core.async.impl.channels.ManyToManyChannel %))
(s/def ::halt ::chan)
(s/def ::xf (s/keys :req-un [::xf]))
(s/def ::next (s/nilable ::pipe))
(s/def ::pipe (s/keys :req-un [::xf ::next]))
(s/def ::xfs (s/and (s/coll-of ::xf) seq))
(s/def ::worker-opts (s/keys :opt-un [::queue-count ::hook ::halt ::apply-xf ::enqueue]))
(s/def ::source (s/or :buffered-reader u/buffered-reader?  :coll coll? :channel u/channel? :fn fn?))

(s/fdef worker
  :args (s/cat  :thread-count ::thread-count
                :opts ::worker-opts)
  :ret ::chan)

(s/fdef as-pipe
  :args (s/cat  :xfs ::xfs
                ::ofsett pos-int?)
  :ret ::pipe)

(s/fdef flow
  :args (s/cat  :in ::chan
                :pipe ::pipe
                :worker ::chan
                :opts ::flow-opts)
  :ret ::chan)

;; (stest/instrument
;;  `[worker
;;    as-pipe
;;    flow])


(comment
  (worker 1 nil))
   ;; (u/assert-spec ::worker-args {:thread-count thread-count :opts opts} )

  ;; (s/alt :nullary (s/cat)
  ;;              :unary (s/cat :config ::config))