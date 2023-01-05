(ns test)

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