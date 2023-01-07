(ns pipeline.util
  (:require [clojure.core.async :as a]
            [clojure.core.async.impl.protocols :as async-impl]
            [clojure.spec.alpha :as s]
            [taoensso.timbre :as log]))

(def noop (constantly nil))

(defn assert-spec [spec data]
  (assert (s/valid? spec data) (s/explain-str spec data)))

(defn log-count [msg n]
  (let [cnt (atom 0)]
    (fn []
      (let [new-cnt (dec (swap! cnt inc))]
        (when (zero? (mod new-cnt n))
          (tap> {:count new-cnt})
          (log/info msg :count new-cnt))))))

(defn combine-xfs [xfs]
  (let [{:keys [last-xf xfs]}
        (reduce (fn [{:keys [last-xf] :as acc}
                     {:keys [f] :as xf}]
                  (if (:mult last-xf)
                    (-> (update acc :xfs conj last-xf)
                        (assoc :last-xf xf))
                    (assoc acc :last-xf (update xf :f #(comp % (:f last-xf))))))
                {:last-xf (first xfs)
                 :xfs     []}
                (rest xfs))]
    (conj xfs last-xf)))

(defn channel?
  "Test if something is a channel by checking for certain interfaces."
  [c]
  (and (satisfies? async-impl/ReadPort c)
       (satisfies? async-impl/WritePort c)
       (satisfies? async-impl/Channel c)))

(defn buffered-reader? [source]
  (instance? java.io.BufferedReader source))

(defn channeled
  "Converts a source into a channel. Accepts as source BufferedReaders,
   channels, collections or a function. Function should return an accepted
   source. Returned channel will return, at most, n items and then be closed if
   n is not nil. When source is exhausted channel will be closed, except when
   source is already a channel."
  [source n]
  (let [source-as-channel (a/chan 500)]
    (if (fn? source)
      (channeled (source) n)
      (do (cond
            (buffered-reader? source)
            (a/onto-chan!! source-as-channel (line-seq source) true)

            (coll? source)
            (a/onto-chan! source-as-channel source)

            (channel? source)
            (a/pipe source source-as-channel)

            :else
            (throw (ex-info (str "Pipeline's source must be either a BufferedReader, channel, "
                                 "function, or a coll.\n"
                                 "Got " (type source) ".")
                            {:source source})))
          (cond->> source-as-channel
            n (a/take n))))))

(defn poll-thread-count [thread-count halt thread-i]
  (loop []
    (when (and (>= thread-i @thread-count))
      (let [[_ c] (a/alts!! [(a/timeout 1000) halt]) ]
        (when (not= c halt)
          (recur))))))


(defn out->promises [out on-processed]
  (let [ promises {:result (promise):error (promise) :nil (promise)}]
    (a/go-loop [collect nil]
      (if-let [{:keys [status] :as x} (a/<! out)]
        (recur (on-processed #(update collect status conj x) x status))
        (doseq [[out-type p] promises]
          (deliver p (or (get collect out-type) :done)))))
    promises))

(defn consume-channel [c]
  (a/go-loop []
    (when (a/<! c)
      (recur))))

;; (poll-thread-count thread-i thread-count halt)


(comment
  (defn pipe
  "Takes elements from the from channel and supplies them to the to
  channel. By default, the to channel will be closed when the from
  channel closes, but can be determined by the close?  parameter. Will
  stop consuming the from channel if the to channel closes"
  ([from to] (pipe from to true))
  ([from to close?]
     (go-loop []
      (let [v (<! from)]
        (if (nil? v)
          (when close? (close! to))
          (when (>! to v)
            (recur)))))
     to))
  (defn csv-map
    "Converts rows from a CSV file with an initial header row into a
   lazy seq of maps with the header row keys (as keywords). The 0-arg
   version returns a transducer."
    ([]
     (fn [xf]
       (let [hdr (volatile! nil)]
         (fn
           ([]
            (tap> :empty)
            (xf))
           ([result]
            (tap> {:result result})
            (xf result))
           ([result input]
            (tap> {:result result
                   :input input})
            (let [in-split (str/split input #",")]
              (if-let [h @hdr]
                (xf result (zipmap h in-split))
                (do (vreset! hdr (map keyword in-split))
                    result))))))))

    ([coll]
     (let [[header & records] coll
           hdr (map keyword (str/split header #","))]
       (map #(zipmap hdr (str/split % #",")) records)))))