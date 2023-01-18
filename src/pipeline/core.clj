(ns pipeline.core
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as s]
            [pipeline.protocol :as p]
            [clojure.spec.test.alpha :as stest]
            [pipeline.util :as u]))

(defn apply-xf
  "Actually calls the xf function on data and updates pipe to the next one.
   Handler functions passed to wrapper together with x"
  [{:keys [data pipe] {:keys [xf mult]} :pipe :as x} result-chan]
  (let [result (try
                 (let [result (xf data)]
                   (if mult
                     (if (seq result) result [nil])
                     [result]))
                 (catch Throwable t [t]))
        x' (assoc x :pipe (:next pipe))]
    (a/onto-chan! result-chan (map #(assoc x' :data %) result))))

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

(defn flow-impl
  "Process messages from input in parallel Note: the order of outputs may not
   match the order of inputs."
  [{:keys [threads] :as w} in pipe {:keys [out close?] :or {close? true out (a/chan)}}]
  (let [monitor (atom 0)
        check-in #(swap! monitor inc)
        check-out #(when (and (zero? (swap! monitor dec)) close?)
                     (a/close! out))
        pipe-fn (if (fn? pipe) pipe (constantly pipe))
        input (a/chan 1 (map #(hash-map :data % :pipe (pipe-fn %))))
        ;; input (a/chan)
        ]

    (check-in)
    ;;TODO this can be the input channel with transducer, then pipe into it.
    (a/pipe in input)

    ;; (a/go
    ;;   (loop []
    ;;     (when-let [data (a/<! in)]
    ;;       (tap> {:data data})
    ;;       (a/>! input
    ;;             data
    ;;             ;; {:data data :pipe (pipe-fn data)}
    ;;             )
    ;;       (recur)))
    ;;   (a/close! input))

    (a/go-loop [inputs #{input}]
      (when (seq inputs)
        (let [[{:keys [pipe data] :as x} input] (a/alts! (vec inputs))]
          (if (nil? x)
            (do
              (check-out)
              (recur (disj inputs input))) ;;end of input
            (if (queue? pipe data)
              (let [thread (a/<! threads)] ;;wait for thread to be available
                (check-in)
                (recur (conj inputs (p/start-thread w x thread))))
              (do (a/>! out x)
                  (recur inputs)))))))
    out))



(defrecord Worker [threads thread-count apply-xf]
  p/Threads
  (inc-thread-count [this]
    (when (a/offer! threads :t)
      (swap! thread-count inc)))
  (dec-thread-count [this]
    (let [[old new] (swap-vals! thread-count
                                #(cond-> % (pos? %) dec))]
      (when (< new old)
        (a/go (a/<! threads)))))
  (stop-all [this]
    (loop []
      (when (p/dec-thread-count this)
        (recur))))
  (start-thread [this x thread]
    (let [result (a/chan)]
      (a/thread
        (apply-xf x result)
        (a/>!! threads thread))
      result))
  (flow [this in pipe]
    (flow-impl this in pipe nil)))


(defn create-worker []
  (map->Worker {:threads (a/chan 1000)
                :thread-count (atom 0)
                :apply-xf apply-xf})
  )

(comment


 (future
   (tap> (let [w (create-worker)]
      (p/inc-thread-count w)
      (p/inc-thread-count w)
      ;; (dec-thread-count w)
      (p/stop-all w)
      (-> w :thread-count deref)
      )))

  (let [c (a/chan 2)]
    [(a/offer! c :foo)
     (a/offer! c :foo)
     (a/offer! c :foo)

     (a/poll! c)
     (a/poll! c)
     (a/poll! c)
     (a/poll! c)
     ]
    )
  
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