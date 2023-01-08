(ns pipeline.util
  (:require [clojure.core.async :as a]
            [clojure.core.async.impl.protocols :as async-impl]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [taoensso.timbre :as log]))

(def noop (constantly nil))

(defn assert-spec [spec data]
  (assert (s/valid? spec data) (s/explain-str spec data)))

(defn default-wrapper
  "Expects the update-x fn to be called on x and the result to be returned. To be
   used to hook into pre and post (xf data), eg. for stats or debugging."
  [update-x x]
  (update-x x))

(defn log-count
  "Returns a function that will log msg every n invocations."
  [log msg n]
  (let [cnt (atom nil)]
    (fn []
      (let [new-cnt (dec (swap! cnt (fnil inc 0)))]
        (when (zero? (mod new-cnt n))
          (log [msg :count new-cnt]))))))

(defn log-period [log msg ms]
  (let [t (atom (System/currentTimeMillis))
        cnt (atom 0)]
    (fn []
      (let [old-t @t
            new-t (System/currentTimeMillis)]
        (when (> (- new-t old-t) ms)
          (log [msg :count @cnt])
          (reset! t new-t))
        (swap! cnt inc)))))

(defn combine-xfs
   "TODO"
  [xfs]
  (let [{:keys [last-xf xfs]}
        (reduce (fn [{:keys [last-xf] :as acc} xf]
                  (if (:mult last-xf)
                    (-> (update acc :xfs conj last-xf)
                        (assoc :last-xf xf))
                    (assoc acc :last-xf (update xf :xf #(comp % (:xf last-xf))))))
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

(defn buffered-reader?
   "Test if something is of type BufferReader, such as returned
   from (clojure.java.io/reader \"some file name\")"
  [source]
  (instance? java.io.BufferedReader source))

(defn channeled
  "Converts a source into a channel. Accepts as source BufferedReaders,
   channels, collections or a function. Function should return an accepted
   source. Returned channel will return, at most, n items and then be closed if
   n is not nil. When source is not a channel exhausted returned channel will be
   closed."
  ([source] (channeled source nil))
  ([source n]
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
             n (a/take n)))))))

(defn block-on-pred
   "Polls the b atom every ms milliseconds, returns when (pred a @b) returns false
   or when halt channel is closed."
  [a b pred halt ms]
  (loop []
    (when (pred a @b)
      (let [[_ c] (a/alts!! [(a/timeout ms) halt]) ]
        (when (not= c halt)
          (recur))))))


(defn as-promises
   "TODO"
  ([out] (as-promises out (fn [update-collect _x _status] (update-collect))))
  ([out on-processed]
   (let [ promises {:result (promise):error (promise) :nil-result (promise)}]
     (a/go-loop [collect nil]
       (if-let [{:keys [status] :as x} (a/<! out)]
         (recur (on-processed #(update collect status conj x) x status))
         (doseq [[out-type p] promises]
           (deliver p (get collect out-type)))))
     promises)))

(defn >!!null
  "Reads and discards all values read from c"
  [c]
  (a/go-loop []
    (when (a/<! c)
      (recur))))

(defn linked-list
  "Returns a linked list built from the elements of xs collection with each
   element linked to the next."
  [xs]
  (reduce (fn [ll x]
            (assoc x :next ll))
          (reverse xs)))

(defn csv-xf
  "Takes a csv-source channel, reads the first (headers) element and returns a
   function that will take row elements and returns maps with the headers as
   keywords. Throws is channel is closed or is blocked for ms"
  [csv-source ms]
  (if-let [[header-str _] (a/alts!! [csv-source (a/timeout ms)])]
    (let [headers (map keyword (str/split header-str #","))]
      (fn [row]
        (let [columns (str/split row #",")]
          (zipmap headers columns))))
    (throw (ex-info "No input from csv-source" {}))))

(defn split-by
  "Takes a predicate, a source channel, and a map of channels. Out channel is
   selected looking in the outs map for the result of applying predicate to
   values. Outputs to channel under :default key if not found. The outs will
   close after the source channel has closed."
  [p ch outs]
  (let [{:keys [default] :as outs'}
        (update outs :default  #(or % (a/chan (a/dropping-buffer 1))))]
    (a/go-loop []
      (let [v (a/<! ch)]
        (if (some? v)
          (when (a/>! (get outs (p v) default) v)
            (recur))
          (doseq [out (vals outs')] (a/close! out)))))
    outs'))