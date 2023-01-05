(ns pipeline.core
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as s]
            [pipeline.util :as u]))

(defn poll-thread-count [thread-i thread-count halt]
  (loop []
    (when (and (>= thread-i @thread-count))
      (let [[_ c] (a/alts!! [(a/timeout 1000) halt]) ]
        (when (not= c halt)
          (recur))))))

(defn threads [thread-count max-thread-count queue-count halt]
  (u/assert-spec ::threads-args {:thread-count thread-count :max-thread-count max-thread-count
                                 :queue-count queue-count :halt halt})
  (let [queues (->> (repeatedly a/chan) (take queue-count))
        p-queues (reverse (into (if halt [halt] []) queues))]
    (dotimes [thread-i max-thread-count]
      (a/thread
        (loop []
          (tap> {:thread-i thread-i})
          (poll-thread-count thread-i thread-count halt)
          (let [[x-to-process _] (a/alts!! p-queues :priority true)]
            (when (some? x-to-process)
              (let [xf (-> x-to-process :xfs first)
                    {:keys [xfs done start end data] :as x} ((:wrapped-f xf) x-to-process)]
                (a/go (doseq [data data]
                        (let [status (cond (instance? Throwable data) :error
                                           (empty? xfs)               :result
                                           (nil? data)                :nil
                                           :else                      :queue)
                              x' (assoc x :data  data :status status)]
                          (if (= status :queue)
                            (do  (start)
                                 (a/>! (-> xfs first :queue) x'))
                            (a/>! done x'))))
                      (end)))
              (recur))))
        (tap> {:thread-done thread-i})))
    queues))

(defn try-xf [{:keys [mult f]} data]
  (try
    (let [result (f data)]
      (if mult
        (if (seq result) result [nil])
        [result]))
    (catch Throwable t t)))

(defn pipeline
  ([xfs queues] (pipeline xfs queues nil))
  ([xfs queues {:keys [pre-xf post-xf] :or {pre-xf identity post-xf identity}}]
   (u/assert-spec ::pipeline-args {:xfs xfs :queues queues})
   (assert (>= (count queues) (count xfs))
           (str "Not enough queues (" (count queues) ") for steps in pipeline ("(count xfs) ")"))
   (map (fn [xf queue]
          (merge xf {:wrapped-f (fn [{:keys [data xfs] :as x}]
                                  (-> (pre-xf x)
                                      (assoc :data (try-xf xf data)
                                             :xfs (rest xfs))
                                      post-xf))
                     :queue     queue}))
        xfs queues)))

(defn flow
  ([source xfs] (flow source xfs {}))
  ([source xfs {:keys [on-start on-queue on-processed on-done n]
                :or {on-start u/noop
                     on-queue identity
                     on-processed u/noop
                     on-done u/noop}}]
   (u/assert-spec ::flow-args {:source source :pipeline-xfs xfs})
   (on-start)
   (let [done (a/chan)
         monitor (atom 0)
         {:keys [start] :as wrapper} {:start #(swap! monitor inc)
                                      :end   #(when (zero? (swap! monitor dec))
                                                (on-done)
                                                (a/close! done))
                                      :xfs   xfs
                                      :done  done}
         pre-xf (map (fn [data]
                       (start)
                       (on-queue (assoc wrapper :data data))))
         source-as-channel (u/channeled source pre-xf n)
         promises {:result (promise):error (promise) :nil (promise)}]

     (a/pipe source-as-channel (->> xfs first :queue) false)

     (a/go-loop [collect nil]
       (let [{:keys [status] :as x} (a/<! done)]
         (if (some? x)
           (recur (on-processed #(update collect status conj x) x status))
           (doseq [[out-type p] promises]
             (deliver p (or (get collect out-type) :done))))))
     promises)))

(s/def ::f fn?)
(s/def ::wrapped-f fn?)
(s/def ::mult (s/nilable boolean))
(s/def ::thread-count (comp nat-int? deref))
(s/def ::max-thread-count pos-int?)
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
(s/def ::threads-args (s/keys :req-un [::thread-count ::max-thread-count ::queue-count]
                              :opt-un [::chan]))

(s/def ::pipeline-args (s/keys :req-un [::xfs ::queues]
                               :opt-un []))

(s/def ::source (s/or :buffered-reader u/buffered-reader?  :coll coll? :channel u/channel? :fn fn?))

(s/def ::flow-args (s/keys :req-un [::source ::pipeline-xfs]))