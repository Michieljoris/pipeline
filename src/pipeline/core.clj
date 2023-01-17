(ns pipeline.core
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [pipeline.util :as u]))

(defn apply-xf-c
  "Actually calls the xf function on data and updates pipe to the next one.
   Handler functions passed to wrapper together with x"
  [{:keys [data pipe] {:keys [xf mult]} :pipe :as x}]
  (let [result (try
                 (let [result (xf data)]
                   (if mult
                     (if (seq result) result [nil])
                     [result]))
                 (catch Throwable t [t]))
        x' (assoc x :pipe (:next pipe))]
    (a/to-chan! (map #(assoc x' :data %) result))))

(defn queue?
  "Default implementation. Decide on queuing for further processing."
  [pipe data]
  (not (or (empty? pipe)
           (nil? data))))

(defn as-pipe
  "Prepares xfs (list of maps, each having at least a single argument function
   under the :xf key) so that it can be passed to the flow function."
  ([xfs] (as-pipe xfs 0))
  ([xfs offset]
   (->> xfs (map-indexed #(assoc %2 :i (+ offset %1))) u/linked-list)))

(defprotocol Threads
    (inc-thread-count [this t])
    (dec-thread-count [this])
    (start-thread [this])
    (stop-all [this])
    (flow [this in pipe] [this in pipe opts]))

(defn enqueue-c
  "Default implementation. Enqueue x on the appropriate queue. Queueing should
   block in a go thread. check-in should be called before every queueing,
   check-out should be called after all results are queued"
  [result-channel {:keys [check-in check-out out]}
   ;; {:keys [data pipe] :as x} queues
   ]
  (let [task (a/chan)]
    (a/go-loop []
      (if-let [{:keys [data pipe] :as x} (a/<! result-channel)]
        (do
           ;; let [task (a/chan) ;; (get queues (:i pipe))
           ;;      ]
          (a/>! task x)
            ;; (if (queue? pipe data)
            ;;   (a/>! task x)
            ;;   (a/>! out x))
          (recur))
        (do
          (a/close! task)
          (check-out))))
    task
    ))

(defn apply-xf-p [thread x threads]
  (a/thread
    (let [result-channel (apply-xf-c x)
          task (enqueue-c result-channel (meta x))]
      (a/>!! threads thread)
      task)))

(defn flow-impl2
  "Process messages from input in parallel Note: the order of outputs may not
   match the order of inputs."
  [{:keys [threads]} in pipe {:keys [out close?] :or {close? true out (a/chan)}}]
  (let [monitor (atom 0)
        check-in #(swap! monitor inc)
        check-out #(when (and (zero? (swap! monitor dec)) close?)
                     (a/close! out))
        wrapped-x (with-meta {} {:check-in check-in :check-out check-out :out out})
        pipe-fn (if (fn? pipe) pipe (constantly pipe))
        input (a/chan)]

    (check-in)
    (a/go
      (loop []
        (if-let [data (a/<! in)]
          (do
            (check-in)
            (when (a/>! input (assoc wrapped-x :data data :pipe (pipe-fn data)) )
              (recur)))
          (a/close! input)))
      (check-out))

    (a/go-loop [tasks #{input}]
      (when (seq tasks)
        (let [[{:keys [pipe data] :as x} task] (a/alts! (vec tasks) :priority true)] ;;order by priority?
          (if (nil? x)
            (recur (disj tasks task)) ;;end of input
            (if (queue? pipe data)
              (do
                (check-in)
                (recur (conj tasks (apply-xf-p (a/<! threads) x threads))))
              (do (a/>! out x)
                  (recur tasks)))))))))



(defrecord Worker [queues threads apply-xf enqueue]
  Threads
  (inc-thread-count [this t]
    (a/go (a/>! threads t)))
  (dec-thread-count [this]
    (a/go (a/<! threads)))
  (start-thread [this]
    (a/thread
      (enqueue-c (apply-xf x) queues (meta x))
      (a/>!! threads thread)))
  (stop-all [this]
    (a/go-loop []
      (when (a/<! threads)
        (recur))))
  (flow [this in pipe]
    (flow-impl2 this in pipe nil)))

(defn worker []
  (map->Worker {:threads (a/chan 1000)})
  )

(comment
  
;; (defn enqueue-c
;;   "Default implementation. Enqueue x on the appropriate queue. Queueing should
;;    block in a go thread. check-in should be called before every queueing,
;;    check-out should be called after all results are queued"
;;   [result-channel queues {:keys [check-in check-out out]}
;;    ;; {:keys [data pipe] :as x} queues
;;    ]
;;   (a/go-loop []
;;       (if-let [{:keys [data pipe] :as x} (a/<! result-channel)]
;;         (do
;;           (let [queue (get queues (:i pipe))]
;;             (if (queue? pipe data)
;;               (do  (check-in)
;;                    (a/>! queue x))
;;               (a/>! out x)))
;;           (recur))
;;         (check-out))))



  
  (defn flow-impl
    "Flows in through the pipe using worker, producing 1 or more outputs per input.
   Results are put on the returned out channel, which can be passed in. The
   pipe (or the result of calling pipe on source if pipe is a function) gets
   assoced with every input element and is used by the threads to apply the
   right transformation. Results are unordered relative to input. By default,
   the out channel will be closed when the in channel closes (once all
   processing is done), but this can be determined by the close? parameter.
   Consumes from the in channel as long as data is taken from the out channel or
   until out channel is closed (once all processing is done)."
    [worker in pipe {:keys [out close?] :or {close? true out (a/chan)}}]
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
      out))

  (defrecord Worker [state queues enqueue apply-xf]
    Threads
    (inc-thread-count [this]
      (swap! state conj (start-thread this)))
    (dec-thread-count [threads]
      (when-let [halt (first @state)]
        (swap! state rest)
        (a/close! halt)))
    (start-thread [this]
      (let [halt (a/chan)
            p-queues (into [halt] (reverse queues))]
        (a/thread
          (loop []
            (let [[x _] (a/alts!! p-queues :priority true)]
              (when x
                (enqueue (apply-xf x) queues (meta x))
                (recur)))))
        halt))
    (stop-all [this]
      (doseq [halt @state] (a/close! halt)))
    (flow [this in pipe] (flow this in pipe nil))
    (flow [this in pipe opts] (flow-impl (first queues) in pipe opts)))

  (defn worker
    "Starts up thread-count threads, and creates queue-count queue channels. Each
   thread is set up to process data as put on the queues."
    ([thread-count] (worker thread-count nil))
    ([thread-count {:keys [queue-count apply-xf enqueue]
                    :or   {queue-count 100
                           enqueue     enqueue-c
                           apply-xf    apply-xf-c}}]
     (let [this-worker (map->Worker {:queues   (->> (repeatedly a/chan) (take queue-count) vec)
                                     :state    (atom [])
                                     :enqueue  enqueue
                                     :apply-xf apply-xf})]
       (dotimes [_ thread-count] (inc-thread-count this-worker))
       this-worker))))

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