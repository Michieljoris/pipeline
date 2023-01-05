(ns dev-stats
  (:require
   [pipeline.frequencies :as freq]

   [pipeline.stat :as stat]
   [pipeline.xforms :as xf])
  )

;; more



(def halt (atom true))
(comment
  ;; (periodically mark-period default-period)
  )

(defn update-freq-map [freq-map val]
  (update freq-map val (fnil inc 0)))

(defn update-bucket-map [bucket-map bucket-size val]
  (update bucket-map (freq/bucket bucket-size val) (fnil inc 0)))

;; (-> (update-bucket-map {} 10 1)
;;      (update-bucket-map 1 10)
;;      )



;; (freq/stats (freq/bucket-frequencies 10 [1 4 15 20]))
;; (freq/bucket-frequencies 10 [1 4 15 20])



;; (-> (update-bucket-map {} 10 1)
;;     (update-bucket-map 4 10)
;;     (update-bucket-map 15 10)
;;     (update-bucket-map 20 10)
;;     (bucket-stats 10)
;;     )


;; (add-watch stats-atom :stats (fn [_ _ old new]
;;                           (tap> {:stats-watch {:old old :new new}})
;;                           ))



;; (deref stats-atom)


;; (add-stat (keyword (str (name data-point) "-rate-per-second"))
;;           period)




(comment
  ;; (mark-period)

  ;; (init-stats [{:data-point :foo
  ;;               :bucket-size 10}
  ;;              {:data-point :bar
  ;;               :bucket-size 10}])
  (:foo1 @stat/stats-atom)

  (reset! halt true)
  (do
    (reset! halt nil)
    (stat/periodically stat/mark-period stat/default-period halt))
  (stat/add-stat :foo1 1)
  (stat/add-stat :foo1 4)
  (stat/add-stat :foo1 15)
  (stat/add-stat :foo1 20)
  ;; (reset-stats)
  (stat/stats 3))



(defn throw-err [e]
  (when (instance? Throwable e) (throw e))
  e)

(defmacro <? [ch]
  `(throw-err (a/<! ~ch)))

;; (defn submit*
;;   [pool f]
;;   (let [result (.submit ^ExecutorService pool ^Callable f)]
;;     result))

;; (defmacro submit
;;   [pool & body]
;;   `(submit* ~pool (fn* [] ~@body)))

(comment
  (a/go
    (try
      (<? (a/thread
            (try
              (throw (ex-info "foo" {}))
              (catch Throwable t
                t)
              )
            ))
      (catch Throwable e
        (tap> {:e!!! e}))
      )))


(defn record-thread-details [thread-details {:keys [index xfs]} thread on?]
  (swap! thread-details conj {:thread thread
                              :index index
                              :step (-> xfs first :step)
                              :timestamp (now)
                              :on? on?}))


(def last-timestamp (atom nil))
(def max-thread-count (atom 10))
(do
  (reset! last-timestamp nil)
  (defn thread-activity [xfs]
    (let [thread-count @max-thread-count
          thread-vector (into [] (take thread-count (repeat
                                                     ;; :xxxx
                                                     nil
                                                            )))]
      (->> @thread-details
           (reduce (fn [acc {:keys [thread xfs-i index on? timestamp]}]
                     (when (and @last-timestamp on?)

                     (tap> (- timestamp @last-timestamp))
                       (stat/add-stat :thread-on (- timestamp @last-timestamp))
                       )
                     (reset! last-timestamp timestamp)
                     (let [last-thread-vector (last acc)]
                       (conj acc (assoc last-thread-vector thread (if on?
                                                                      [index (inc xfs-i)]
                                                                      []
                                                                      ;; :xxxx
                                                                      )))))
                   [thread-vector])
           (map (fn [v]
                  (let [v' (into [] (sort (keep second v)))
                        thread-count (count v')
                        freq-map (frequencies v')]
                    (->> (range (count xfs))
                         (reduce (fn [acc xfs-i]
                                   (conj acc
                                         (if-let [freq (get freq-map (inc xfs-i))]
                                           (int (* 100.0 (/ freq thread-count)))
                                           0
                                           ))
                                   )
                                 [(str thread-count)]
                                 ))
                    )

                  ))
           ;; (map #(sort (fn [x y]
           ;;               (cond
           ;;                 (and (vector? x) (keyword? y)) -1
           ;;                 (and (vector? y) (keyword? x)) 1
           ;;                 (and (vector? x) (vector? y)) (compare (first x) (first y))
           ;;                 :else 1)) %))
           ;; (map str)
           )
      ))
  ;; (thread-activity)
  )

;; (defn process-source [{:keys [source xfs]} monitor
;;                       {:keys [cpu-thread-count io-thread-count log complete abort?] :as opts}]
;;   (let [xfs-for-source (map-indexed #(assoc %2 :step %1) xfs)
;;         max-xfs-count (count xfs-for-source)
;;         queues   (->> (repeatedly #(vector (chan) (chan))) (take max-xfs-count) (into [])) ;;NOTE: minimum nr of queues is (max  (map count <all of xfs used>))
;;         source-x (chan 1 (map-indexed (fn [index data]
;;                                         {:data  data
;;                                          :batch 1
;;                                          :xfs   (map #(assoc %1 :queue (if (:blocking %1)
;;                                                                          (first %1) (second %2))) xfs-for-source queues) ;; NOTE: every x could have a different xfs to apply
;;                                          :index index :begin (stat/now)})))
;;         [io-queues cpu-queues] (reduce (fn [[io-queues cpu-queues] [io-queue cpu-queue]]
;;                                          [(conj io-queues io-queue) (conj cpu-queues cpu-queue)])
;;                                        [[] []] queues)]

;;     (a/onto-chan! source-x source)
;;     (process-queues cpu-queues cpu-thread-count monitor opts)
;;     ;; (process-queues cpu-queues io-thread-count monitor opts)

;;     (go-loop [{:keys [xfs batch] :as x} (a/<! source-x)]
;;       (if (and (nil? x) batch)
;;         (watch-monitor monitor batch)
;;         (let [xf (first xfs)
;;               x' (assoc x :status (get-status xf abort? x))]
;;           (log {:submitting x})
;;           (if (= :to-queue (:status x'))
;;             (submit (:queue xf) x')
;;             (when complete (s-put! complete x')))
;;           (recur (a/<! source-x)))))

;;     (let [[io-queue cpu-queue] (get queues (dec (count xfs)))]
;;          (if (:blocking (last xfs)) io-queue cpu-queue))
;;     ))