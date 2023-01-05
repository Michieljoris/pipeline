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
   source. Applies xf to returned channel if not nil. Returned channel will
   return, at most, n items if n is not nil"
  [source xf n]
  (let [source-as-channel (a/chan 500 (cond-> (or xf (map identity))
                                        n (comp (take n))))]
    (if (fn? source)
      (channeled (source) xf n)
      (do (cond
            (buffered-reader? source)
            (a/onto-chan!! source-as-channel (line-seq source))

            (coll? source)
            (a/onto-chan! source-as-channel source)

            (channel? source)
            (a/pipe source source-as-channel)

            :else
            (throw (ex-info (str "Pipeline's source must be either a BufferedReader, channel, "
                                 "function, or a coll.\n"
                                 "Got " (type source) ".")
                            {:source source})))
          source-as-channel))))