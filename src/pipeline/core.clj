(ns pipeline.core
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [pipeline.util :as u]))

(defn apply-xf
  "Default implementation. Calls the pipeline's xf function on wrapped data and
   updates pipeline."
  [{:keys [data pipeline] :as x} result]
  (let [x' (merge x {:data     ((-> pipeline first :xf) data)
                     :pipeline (rest pipeline)})]
    (a/go (a/>! result x') (a/close! result))))

(defn queue?
  "Default implementation. Decide on queueing for further processing."
  [pipeline data]
  (and (seq pipeline) (some? data)))

(defn start-thread
  "Applies apply-xf fn to x in a new thread. Puts itself back in the pool when
   done. Returns channel with result."
  [{:keys [threads]} apply-xf x]
  (let [result (a/chan)]
    (a/thread
      (apply-xf x result)
      (a/>!! threads :t))
    result))

(defn flow
  "Flow source through pipeline using worker to optionally supplied out. When
   close? is true closes out when in closes (and processing is finished). Supply
   optional queue? fn to decided on queuing, and optional custom apply-xf fn.
   Returns out channel "
  [{:keys [threads] :as worker} source pipeline
   {:keys [out close? queue? apply-xf]
    :or {close? true out (a/chan) apply-xf apply-xf queue? queue?}}]
  (let [monitor (atom 0)
        check-in #(swap! monitor inc)
        check-out #(when (and (zero? (swap! monitor dec)) close?)
                     (a/close! out))
        pipeline-fn (if (fn? pipeline) pipeline (constantly pipeline))
        input (a/chan 1 (map #(hash-map :data % :pipeline (pipeline-fn %))))]

    (check-in)
    (a/pipe source input)

    (a/go-loop [inputs #{input}]
      (when (seq inputs)
        (let [[{:keys [pipeline data] :as x} input] (a/alts! (vec inputs))]
          (if (nil? x)
            (do
              (check-out)
              (recur (disj inputs input))) ;;end of input
            (if (queue? pipeline data)
              (do  (a/<! threads) ;;wait for thread to be available
                   (check-in)
                   (recur (conj inputs (start-thread worker apply-xf x))))
              (do (a/>! out x)
                  (recur inputs)))))))
    out))

(defn inc-thread-count
  "Increase thread count of worker"
  [{:keys [threads thread-count]}]
  (when (a/offer! threads :t)
    (swap! thread-count inc)))

(defn dec-thread-count
  "Decrease thread count of worker"
  [{:keys [threads thread-count]}]
  (let [[old new] (swap-vals! thread-count
                              #(cond-> % (pos? %) dec))]
    (when (< new old)
      (a/go (a/<! threads)))))

(defn worker
  "Returns map with stateful worker data."
  ([] (worker nil))
  ([{:keys [max-thread-count]
     :or   {max-thread-count 1000}}]
   {:threads      (a/chan max-thread-count)
    :thread-count (atom 0)}))

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

   ;; (u/assert-spec ::worker-args {:thread-count thread-count :opts opts} )

  ;; (s/alt :nullary (s/cat)
  ;;              :unary (s/cat :config ::config))