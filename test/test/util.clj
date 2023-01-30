(ns test.util
 (:require
   [pipeline.util :as u]))

(defn rand-sleep [ms variance]
  (let [x (- ms variance)
        y (+ ms variance)]
    (Thread/sleep (+ x (int (rand (- y x)))))))

(defn prime? [n]
  (letfn [(divides? [m n] (zero? (rem m n)))]
    (and (< 1 n) (not-any? #(divides? n %) (range 2 n)))))

(defn work [] ;;about 20ms
  (dotimes [i 2400]
    (prime? i)))

(defn rand-work [ms variance]
  (let [work-units (/ ms 20)
        var-units (/ variance 20)
        x (- work-units var-units)
        y (+ work-units var-units)]
    (dotimes [_ (+ x (int (rand (- y x))))]
      (work))))

(def group-by-result-type
  (comp (partial group-by (fn [{:keys [data pipeline] :as x}]
                            (cond (instance? Throwable data) :error
                                  (empty? pipeline)        :result
                                  (nil? data)                :nil-result
                                  :else                      :queue)))
        #(sort-by :i %)))

(defn extract-raw-results [out]
  (-> out
      u/as-promise
      deref
      group-by-result-type))

(defn extract-results [out]
(reduce-kv (fn [acc k v]
             (assoc acc k (mapv :data v)))
        {}
        (extract-raw-results out)))

(defn wrap-apply-xf [apply-xf]
  (fn [x]
    ((:log-count (first (:pipeline x)) #(do)))
    ((:log-period (first (:pipeline x)) #(do)))
    (apply-xf x)))

(defn =tap [a b]
  (tap> {:result a :expected b})
  (= a b))
