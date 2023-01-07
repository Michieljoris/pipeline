(ns pipeline.core
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as s]
            [pipeline.util :as u]))

(defn threads
  "Starts up thread-count threads, and creates queue-count queue channels. Each
   thread is set up to process data as put on the queues. Returns a map with the
   queues and a halt channel, that when closed stops all threads. The
   process-hook function gets called with the thread number every time the
   thread starts any work."
  [thread-count queue-count process-hook]
  (u/assert-spec ::threads-args {:thread-count thread-count :queue-count queue-count :process-hook process-hook})
  (let [halt (a/chan)
        queues (->> (repeatedly a/chan) (take queue-count))
        p-queues (reverse (into [halt] queues))]
    (dotimes [thread-i thread-count]
      (a/thread
        (loop []
          ;; (process-hook thread-i)
          (let [[x-to-process _] (a/alts!! p-queues :priority true)]
            (when (some? x-to-process) ;;highest priority halt can be closed->thread finishes
              (let [f (-> x-to-process :xfs first :wrapped-f)
                    {:keys [xfs out queue dequeue data] :as x} (f x-to-process)]
                (a/go (doseq [data data]
                        (let [status (cond (instance? Throwable data) :error
                                           (empty? xfs)               :result
                                           (nil? data)                :nil
                                           :else                      :queue)
                              x' (assoc x :data  data :status status)]
                          (if (= status :queue)
                            (do
                             (tap> {:queue x'})
                              (queue)
                              (a/>! (-> xfs first :queue) x')
                              (tap> {:done-queuing x'})
                              )
                            (a/>! out x'))))
                      (do
                        (tap> {:dequeue x})
                        (dequeue))))
              (recur))))
        (tap> {:thread-done thread-i})))
    {:queues queues :halt halt}))

(defn- try-xf [{:keys [mult f]} data]
  (try
    (let [result (f data)]
      (if mult
        (if (seq result) result [nil])
        [result]))
    (catch Throwable t [t])))

(defn pipe
   "Takes a collection of maps (each defining a transformation) and queues (as
   returned by the threads fn) and returns a pipe that can be passed to the flow
   fn, or assigned to an input element in a pre-xf or post-xf hook. A pipe is
   the list of transformation functions with assigned queues and potential
   hooks"
  [xfs queues hooks]
  (u/assert-spec ::pipeline-args {:xfs xfs :queues queues :hooks hooks})
  (let [xfs (map (fn [xf queue] (assoc xf :queue queue)) xfs queues)]
    (loop [xfs' [] [xf & rest-xfs] xfs]
      (if xf
        (let [_ (assert (:queue xf) (str "Not enough queues (" (count queues) ") for steps in pipeline ("(count xfs) ")"))
              pre-xf (or (:pre-xf xf) (:pre-xf hooks) identity)
              post-xf (or (:post-xf xf) (:post-xf hooks) identity)
              ;;TODO: fix rest of xfs don't have the :wrapped-f ....
              xf (assoc xf :wrapped-f (fn [{:keys [data] :as x}]
                                        (-> (pre-xf x)
                                            (assoc :data (try-xf xf data)
                                                   :xfs rest-xfs)
                                            post-xf)))]
          (recur (conj xfs' xf) rest-xfs))
        xfs'))))

(defn flow
  "Takes elements from the in channel and supplies them to the out channel,
   producing 1 or more outputs per input. The pipe gets assoced with every input
   element and is used by the threads to apply the right transformation. Results
   are unordered relative to input. By default, the to channel will be closed
   when the from channel closes, but can be determined by the close? parameter.
   Will stop consuming the in channel if the out channel closes."
  [in pipe out {:keys [on-queue close?]
                :or {on-queue u/noop
                     close? true}}]
  (u/assert-spec ::flow-args {:source in :pipeline-xfs pipe})
  (let [monitor (atom 0)
        _ (add-watch monitor :monitor (fn [k m o n]
                                        (tap> {:monitor n})
                                        ))
        {:keys [queue dequeue] :as x} {:queue   #(swap! monitor inc)
                                       :dequeue #(when (zero? (swap! monitor dec))
                                                   (tap> {:done :!!!!})
                                                   (when close? (a/close! out)))
                                       :xfs     pipe
                                       :out     out}]

    (tap> :init-queue)
    (queue)
    (a/go
      (loop []
        (if-let [data (a/<! in)]
          (let [x' (assoc x :data data)]
            (tap> {:source-queue x})
            (queue)
            (when (a/>! (->> pipe first :queue) x')
              (on-queue x)
              (recur)))
          (do
            (tap> :dequeue-source)
            (dequeue)))))

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

(a/go
  (let [c (a/chan)]
    (a/close! c)
    (tap> :putting)
    (tap> {:result (a/>! c 123)})
    (tap> :done)
    ))