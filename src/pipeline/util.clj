(ns pipeline.util
  (:require [clojure.core.async :as a]
            [clojure.core.async.impl.protocols :as async-impl]
            [clojure.string :as str]
            [taoensso.timbre :as log]))

(def noop (constantly nil))

(defn periodically
  "Executes f every period (in ms) till halt channel is closed"
  [f ms halt]
  (a/go-loop []
    (let [[_ c] (a/alts! [(a/timeout ms) halt])]
      (when-not (= c halt)
        (f)
        (recur)))))

(defn log-count
  "Returns a function that will log msg every n invocations."
  [log msg n]
  (let [cnt (atom nil)]
    (fn []
      (let [new-cnt (dec (swap! cnt (fnil inc 0)))]
        (when (zero? (mod new-cnt n))
          (log [msg :count new-cnt]))))))

(defn ms->duration [ms]
    (let [hours (quot ms (* 60 60 1000))
          minutes (- (quot ms (* 60  1000)) (* hours 60))
          seconds (- (quot ms 1000) (* minutes 60) (* hours 60 60))]
      (str hours "h:" minutes "m:" seconds "s" (when (< ms 1000) (str ":" ms "ms") ) )))

(defn log-period [log msg ms]
  (let [start (System/currentTimeMillis)
        t (atom start)
        cnt (atom 0)]
    (fn []
      (swap! cnt inc)
      (let [new-t (System/currentTimeMillis)
            [old-t new-t]
            (swap-vals! t (fn [old-t]
                            (if (> (- new-t old-t) ms)
                              new-t
                              old-t)))]
        (when (not= old-t new-t)
          (log [msg :cnt @cnt :elapsed (ms->duration (- new-t start))]))))))

(defn combine-xfs
  "Takes a collection of xf maps and combines and reduces the number of
   transforming functions through composition. Xf maps that have :mult key set
   to true are not composed with the following xf."
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
  "Polls the a atom every ms milliseconds, returns when (pred a) returns false
   or when halt channel is closed."
  [a pred halt ms]
  (loop []
    (when (pred a)
      (let [[_ c] (a/alts!! [(a/timeout ms) halt]) ]
        (when (not= c halt)
          (recur))))))

(defn as-promises
  "Returns a map with two promises, result and nil-result that deliver all
   elements as taken from out."
  [out]
  (let [promises {:result (promise) :nil-result (promise)}]
    (a/go-loop [collect nil]
      (if-let [{:keys [data] :as x} (a/<! out)]
        (recur (let [status (if data :result :nil-result)]
                 (update collect status conj x)))
        (doseq [[out-type p] promises]
          (deliver p (get collect out-type)))))
    promises))

(defn as-promise
  "Returns promise that delivers all results taken from out."
  [out]
  (let [p (promise)]
    (a/go-loop [collect nil]
      (if-let [x (a/<! out)]
        (recur (conj collect x))
        (deliver p collect)))
    p))

(defn sink
  "Returns an atom containing a vector. Consumes values from channel
  ch and conj's them into the atom."
  [ch]
  (let [a (atom [])]
    (a/go-loop []
      (let [val (a/<! ch)]
        (when-not (nil? val)
          (swap! a conj val)
          (recur))))
    a))

(defn drain
  "Reads and discards all values read from c"
  [c]
  (a/go-loop []
    (when (a/<! c)
      (recur))))

(defn csv-xf
  "Takes a csv-source channel, reads the first (headers) element and returns a
   function that will take row elements and returns maps with the headers as
   keywords. Throws if channel is closed or is blocked for ms milliseconds"
  [ms csv-source]
  (if-let [[header-str _] (a/alts!! [csv-source (a/timeout ms)])]
    (let [headers (map keyword (str/split header-str #","))]
      (fn [row]
        (let [columns (str/split row #",")]
          (zipmap headers columns))))
    (throw (ex-info "No input from csv-source" {}))))

