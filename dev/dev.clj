(ns dev
  (:require
   [test :as test]
   [clojure.core.async :as a]
   [clojure.java.io :as io]
   [pipeline.core :as p]
   [pipeline.stat :as stat]
   [pipeline.util :as u]
   [clojure.string :as str]
   [clojure.data.csv :as csv]
   [taoensso.timbre :as log]
   [pipeline.impl.minimal :as m]
   [clj-http.client :as client]
   [org.httpkit.client :as http]
   [pipeline.impl.instrumented :as i]
   [pipeline.impl.default :as d])
  )


(def group-by-result-type
  (comp (partial group-by (fn [{:keys [data pipeline] :as x}]
                            (cond (instance? Throwable data) :error
                                  (empty? pipeline)          :result
                                  (nil? data)                :nil-result
                                  :else                      :queue)))
        #(sort-by :i %)))

(defn extract-raw-results [out]
  (-> out
      u/as-promise
      deref
      group-by-result-type))

(defn apply-xf
  "Actually calls the xf function on data and updates pipe to the next one.
   Returns channel with (possilbe) multiple results. Catches any errors and
   assigns them to the :data key."
  [{:keys [data pipeline] :as x}]
  (let [datas (try
                (let [{:keys [xf mult async]} (first pipeline)
                      result (xf data)]
                  (if mult
                    (if (seq result) result [nil])
                    [result]))
                (catch Throwable t [t]))
        x' (assoc x :pipeline (rest pipeline))]
    (a/to-chan! (map #(assoc x' :data %) datas))))

(defn work
  "Receives wrapped data as x, should call apply-xf on x asynchronously and then
   done, and return a channel with results."
  ([{:keys [data pipeline] :as x} done]
   (let  [{:keys [xf async]} (first pipeline)]
     (if async
       (let [result (a/chan 1 (map #(merge x {:data %
                                              :pipeline (rest pipeline)})))
             cb (fn [res]
                  (a/go (a/>! result res)
                        (a/close! result)))]
         (xf data cb)
         (done)
         result)
       (work apply-xf x done))))
  ([apply-xf x done]
   (let [result (a/chan)]
     (a/thread (a/pipe (apply-xf x) result) (done))
     result)))

;; DONE update tests
;; TODO finish specs
;; TODO: finish readme

;; TODO: clean up dev

;; Setting thread count
;; TODO: Adjust number of threads on the fly!!!!
;; TODO: auto adjust thread count to max throughput, minimal threads, then set to 80% for example
;; TODO: auto adjust thread count to x % cpu load

;; Testing for real
 ;; TODO: benchmark and instrument!!!!
 ;; TODO: add throttler

;; Stats
;; ------- test/showcase functionality
;; TODO: log estimate on likely duration of job
;; TODO add stats examples


(def mx-bean (java.lang.management.ManagementFactory/getThreadMXBean))
(def os-bean (java.lang.management.ManagementFactory/getOperatingSystemMXBean))
(def run-time (java.lang.Runtime/getRuntime))

(comment
  (let [

        thread-info (.dumpAllThreads mx-bean false false)]
    (.getThreadId (second thread-info))

    (/ (.getCurrentThreadCpuTime mx-bean) (* 1000.0 1000 1000))
    (doseq [thread-id (into [ ] (.getAllThreadIds mx-bean))]

      ;; (tap> (/ (.getThreadCpuTime mx-bean thread-id) (* 1000.0 1000 1000)))


      )
    (tap> {:supported? (.isCurrentThreadCpuTimeSupported mx-bean)})
    (tap> {:enabled? (.isThreadCpuTimeEnabled mx-bean)})
    (tap> {:current-thread-id (.getId (Thread/currentThread))})

    (tap> (/ (.getThreadCpuTime mx-bean 26) (* 1000.0 1000 1000)))

    )

  (.getCpuLoad os-bean)
  (/ (.getTotalMemorySize os-bean) 1000000000.0)
  (let [used-swap (- (/ (.getTotalSwapSpaceSize os-bean) 1000000000.0)
                     (/ (.getFreeSwapSpaceSize os-bean) 1000000000.0))

        free-mem (/ (.getFreeMemorySize os-bean) 1000000000.0)

        ]
    {:used-swap used-swap
     :free-mem free-mem
     :free-mem2 (- free-mem used-swap)}

    )

  (* (/ (.getFreeMemorySize os-bean) 1000000.0) 4)


  (.availableProcessors run-time)

  (doseq [method (into [] (.getDeclaredMethods (.getClass mx-bean)))]
    (tap> method)

    )
  (doseq [method (into [] (.getDeclaredMethods (.getClass os-bean)))]
    (tap> method)

    ))

(def group-by-result-type
  (comp (partial group-by (fn [{:keys [data pipe]}]
                       (cond (instance? Throwable data) :error
                             (empty? pipe)              :result
                             (nil? data)                :nil-result
                             :else                      :queue)))
        #(sort-by :i %)))

(defn extract-raw-results [out]
  (-> out
      u/as-promise
      deref
      group-by-result-type
      ))

(defn extract-results [out]
(reduce-kv (fn [acc k v]
             (assoc acc k (mapv :data v)))
        {}
        (extract-raw-results out)))

(defn apply-xf-fn [apply-xf]
  (let [i (atom -1)]
    (fn [x]
      ((:log-count (:pipe x) #(do)))
      ((:log-period (:pipe x) #(do)))
      (apply-xf
       (cond-> x
         (not (:i x)) (assoc :i (swap! i inc)) )))))

(defn stop [dec-thread-count]
  (loop [] (when (dec-thread-count) (recur))))

(defmacro try-future [& body]
  `(future
     (try
       ~@body
      (catch Exception e# (tap> e#)))))

(defn wrap-apply-xf [apply-xf]
    (let [i (atom -1)]
      (fn [x]

        (stat/add-stat :queued (- (stat/now) (or (:result-queued x) (:queued x))))
        (let [{:keys [log-count log-period]
               :or {log-count u/noop
                    log-period u/noop} :as pipe} (first (:pipeline x))]
          (log-count)
          (log-period)
          (let [now (stat/now)
                result-channel (a/chan 1 (map #(assoc % :result-queued (stat/now))))
                c (apply-xf
                   (cond-> x
                     (not (:i x)) (assoc :i (swap! i inc))))]
            (stat/add-stat (keyword (str "xf-" (:i pipe))) (- (stat/now) now))
            (a/pipe c result-channel)
            result-channel
            ;; c
            )))))

  ;;   L = λ * W

  ;; L - the number of requests processed simultaneously
  ;; λ – long-term average arrival rate (RPS)
  ;; W – the average time to handle the request (latency)

  ;; throughput = thread-count / time-in-system

  (defn optimal-thread-count
    [core-count blocked-time work-time]
    (* core-count  (+ 1 (/ blocked-time work-time))))


  ;; through-put = thread-count / processing-time
  ;; thread-count = through-put * processing-time
  ;; processing-time = thread-count / through-put

  (defn ll-through-put
    "Little's Law"
    [thread-count processing-time]
    (/ thread-count processing-time))

  (defn ll-thread-count
    "Little's Law"
    [through-put processing-time]
    (* through-put processing-time))

  (defn ll-processing-time
    "Little's Law"
    [thread-count through-put]
    (/ thread-count through-put))

(defn number-input [input]
    (let [numbered (a/chan 1 (map-indexed (fn [i x] (assoc x :i i))))]
      (a/pipe input numbered)))

(defn reset-stats []
    (swap! stat/stats-atom (fn [stat-maps]
                        (reduce-kv (fn [acc data-point {:keys [bucket-size]}]
                                     (assoc acc data-point {:data {}
                                                            :bucket-size {}}))
                                   {}
                                   stat-maps))))

(defn read-csv
  [file-name]
  (with-open [reader (io/reader file-name)]
    (doall
     (csv/read-csv reader))))

(defn csv->maps [csv]
  (let [columns (map keyword (first csv))
        members (rest csv)]
    (map #(zipmap columns %) members) ))

(def halt-c (atom nil))
(def out-a (atom nil))

(def work-load (atom 60))
(reset! work-load 10)

(comment

  (ll-thread-count 40 0.16)

  (optimal-thread-count 1 100 100)

  (ll-through-put 2 1)

  (/ (.maxMemory run-time) 1000000.0)

  (try-future
   (tap> (->> (p/flow (m/wrapped (u/channeled (range 1)) [{:xf inc}])
                      (m/tasks 1)
                      {:queue? m/queue?
                       :work m/work})
              extract-raw-results
              ))

   )

  (try-future
   (tap> (->> (p/flow (d/wrapped (u/channeled (range 1)) [{:xf inc}])
                      (d/tasks 1)
                      {:queue? d/queue?
                       :work d/work}
                      )
              extract-raw-results
              ))

   )

  (try-future
   (tap> (->> (p/flow (i/wrapped (u/channeled (range 1)) [{:xf inc}])
                      (d/tasks 1)
                      {:work (partial d/work i/apply-xf)}
                      )
              extract-raw-results
              ))

   )

 (def low (atom nil))
 (def high (atom nil))

(defn delta [a]
  (let [d (first @a)]
    (swap! a (if (nil? d)
               (constantly [1 0])
               #(take 2 (cons (apply + %) %))))
    (or d 0)))

;; (bar low)
;; @low
 (defn foo [tasks min-cpu-load max-cpu-load]
   (fn []
     (let [cpu-load (.getCpuLoad os-bean)]
       ;;TODO: how about memory usage?
       (cond
         (< cpu-load min-cpu-load)
         (let [d (delta low)] ;;TODO no point in upping tasks when it's at maximum!!!
           (dotimes [_ d] (d/inc-task-count tasks))
           (reset! high nil)
           ;; (d/inc-task-count tasks)
           (tap> {:increasing-task-count {:cpu-load cpu-load
                                          :min-cpu-load min-cpu-load
                                          :delta d
                                          :new-task-count @d/task-count}}))

         (> cpu-load max-cpu-load)
         (let [d (delta high)] ;;TODO no point in lowering tasks when it's at 0!!!!
           (dotimes [_ d]  (d/dec-task-count tasks))
           ;; (d/dec-task-count tasks)
          (reset! low nil)
           (tap> {:decreasing-task-count {:cpu-load cpu-load
                                          :max-cpu-load max-cpu-load
                                          :delta d
                                          :new-task-count @d/task-count}}))
         :else (do
                 (reset! low nil)
                 (reset! high nil)))

       (tap> (str "Load: " cpu-load)))))

  (try-future

   (time
    (let [halt  (stat/init-stats 60)
          _ (reset! halt-c halt)
          _ (reset! low nil)
          _ (reset! high nil)
          start-time   (stat/now)
          ;; _ (reset! wrapped/task-ratios [])
          ;; _ (reset! wrapped/cpu-time-a {:last-time 0 :collect []})
          tasks (d/tasks 10)
          _ (def tasks tasks)
          xfs [{:xf (fn [data]
                      (Thread/sleep 50)
                      ;; (test/rand-work 200 100)
                      ;; (tap> {:done data})
                      ;; (Thread/sleep 1000)
                      ;; (/ 1 0)

                      ;; (tap> {:xf data} )

                      (inc data))
                :pre-xf (u/log-period tap>  "I'm alive!!", 5000)
                :i 0
                }
               {:xf inc
                :i 1}
               {:xf (fn [data]
                      ;; (Thread/sleep 500)
                      (test/rand-work @work-load 0)
                      ;; (tap> {:done data})
                      (Thread/sleep 50)
                      ;; (/ 1 0)

                      ;; (tap> {:xf2 data} )
                      data
                      )
                :i 2
                }

               ;; {:xf (fn [data]
               ;;        [(inc data) (inc data)]
               ;;        ;; (inc data)
               ;;        )
               ;;  :mult true}
               ;; {:xf inc}
               ;; {:xf inc}
               ]
          source (i/wrapped (u/channeled (range 1000000)) xfs)
          out (i/out)
          max-cpu-load 0.60
          min-cpu-load 0.50]
      (reset! out-a out)
      (u/periodically (fn []
                        (tap> {:throughput (stat/stats :xf-0)}))
                      5000
                      halt)
      (u/periodically (foo tasks min-cpu-load max-cpu-load) 1000 halt)
      (tap> (->
             (p/flow source tasks
                     {:out out
                      :work   (partial d/work
                                       (partial i/apply-xf :i))})
             extract-raw-results
             :result
             count
             ))

      (tap> {:stats {:xf-0 (stat/stats :xf-0)
                     :xf (stat/stats :xf)
                     :in-system (stat/stats :in-system)
                     :wait (stat/stats :wait)
                     }
             :duration (/ (- (stat/now) start-time) 1000.0)})

      (a/close! halt)
      )
    )
   )


  (.getCpuLoad os-bean)

  ;; Stop all
  (future
    (stop #(d/dec-task-count tasks))
    (a/close! @out-a)
    (a/close! @halt-c))

  (future (tap> (d/inc-task-count tasks)))
  (future (tap> (d/dec-task-count tasks)))

  (read-csv "resources/test.csv"))

(declare work-thread)

(defn threadpool
  [id n]
  (let [chan (a/chan)
        state (atom {:threads-count n})
        inc-threads (fn []
                    (swap! state (fn [{:keys [close-fns] :as s}]
                                   (update s :close-fns conj (work-thread (keyword (str (name id) "-") (str (count close-fns))) chan)))))
        dec-threads (fn []
                      (swap! state (fn [{:keys [close-fns] :as s}]
                                     (when (> (count close-fns) 0)
                                       (update s :close-fns (fn [close-fns]
                                                              ((last close-fns))
                                                              (butlast close-fns)))))))
        start-all (fn []
                  (swap! state (fn [{:keys [threads-count close-fns] :as s}]
                                (when (zero? (count close-fns))
                                  (assoc s :close-fns (mapv #(work-thread (keyword (str (name id) "-") (str %)) chan) (range threads-count)))))))]
    (start-all)
    {:chan        chan
     :stop-all    (fn []
                    (swap! state (fn [{:keys [close-fns] :as s}]
                                (when (> (count close-fns) 0)
                                  (doseq [f close-fns]
                                    (f))
                                  (assoc s :close-fns nil)))))
     :start-all   start-all
     :set-threads (fn [n]
                    (let [{:keys [close-fns]} @state
                          d                   (- (count close-fns) n)]
                      (cond
                        (pos? d) (dotimes [_ d] (dec-threads))
                        (neg? d) (dotimes [_ (- d)] (inc-threads)))))
     :inc-threads inc-threads
     :dec-threads dec-threads}))

(defn flow
  "Flow source through pipeline using tasks to optionally supplied out. When
   close? is true closes out when in closes (and processing is finished).

   The work function receives two arguments, x and done. The function should
   return as fast as possible (so do any actual work in a thread, go block or
   put to result channel in a callback) and return a channel with zero or more
   results, which should close once all results are put on the channel. Once
   work is finished done, a no arg function, should be called so the task can be
   released. Tasks should be channel with filled buffer sized to desired maximum
   concurrency/parallelism task count.

   Returns out channel "
  ([source] (flow source nil))
  ([source {:keys [out close? queue? work]
            :or   {close? true     out  (a/chan)
                   queue? d/queue? work d/work}}]
   (a/go-loop [inputs (cond-> source
                        (not (sequential? source)) vector)] ;;[source xf1-output xf2-output etc], where each vomit x's.
     (if (seq inputs)
       (let [[x input] (a/alts! inputs :priority true)] ;;move-things along using priority
         (recur (if (nil? x) ;; input has been closed, it's been exhausted
                  (remove #(= input %) inputs)
                  (if (queue? x)
                    (conj inputs (work x))
                    (do (a/>! out x)
                        inputs)))))
       (when close? (a/close! out))))
   out))

(defn apply-xf
  "Actually calls the xf function on data and updates pipe to the next one.
   Returns channel with (possilbe) multiple results. Catches any errors and
   assigns them to the :data key."
  [{:keys [data pipeline] :as x}]
  (let [datas (try
                (let [{:keys [xf mult]} (first pipeline)]
                  (d/process-mult mult (xf data)))
                (catch Throwable t [t]))]
    (a/to-chan! (map #(merge x {:data %
                                :pipeline (rest pipeline)}) datas))))

(defn work-thread
  [id chan]
  (let [close (a/chan)]
    (a/thread
      (tap> {id "started-work"})
      (loop []
        (let [[x _] (a/alts!! [chan close])]
          (when-let [[f output] x]
            (a/pipe (f) output)
            (recur))))
      (tap> {id "work done"}))
    #(a/close! close)))

(defn next-output
  [apply-xf {:keys [pipeline] :as x}]
  (let [{:keys [pool]} (first pipeline)
        output (a/chan)]
    (a/go (a/>! pool [#(apply-xf x) output]))
    output))

(defn work
  "Receives wrapped data as x, should call apply-xf on x asynchronously and then
   done, and return a channel with results."
  ([x]
   (work apply-xf x))
  ([apply-xf {:keys [pipeline] :as x}]
   (let [pipeline (first pipeline)]
     (if (sequential? pipeline)
       (a/merge (map #(next-output apply-xf (assoc x :pipeline %)) pipeline))
       (next-output apply-xf x)))))

(comment
  (defn wrap-and-number [source pipeline]
    (let [input (d/wrapped source pipeline)
          numbered (a/chan 1 (map-indexed (fn [i x] (assoc x :i i))))]
      (a/pipe input numbered)))

 (let [{cpu-chan :chan} (threadpool :cpu 1)
      {io-chan :chan} (threadpool :io 1)]
   (future
     (tap> (->> (flow (wrap-and-number (u/channeled [{:e 1}
                                                     {:e 2}
                                                     {:e 3}
                                                     ;; [:e4]
                                                     ;; [:e5]
                                                     ;; [:e6]
                                                     ])
                                       [{:xf   (fn [data]
                                                 (tap> {:xf1 data})
                                               (Thread/sleep 100)
                                        ; (Thread/sleep 1000)
                                               ;; (Thread/sleep 100)
                                               ;; (Thread/sleep 1000)
                                               ;; (if (= data [:e2])
                                               ;;   [[:a1] [:a2]]
                                               ;   (conj data :xf1))
                                                 (assoc data :xf1 true)
                                                 ;; [data data]
                                                 
                                                 )
                                         ;; :mult true
                                         :pool io-chan}
                                        [[{:xf   (fn [data]
                                                   (tap> {:xf2 data})
                                                   ;; (Thread/sleep 200)
                                                   ;; (when (= data {:e 2 :xf1 true})
                                                   ;;   (throw (ex-info "Error" {:data data}))
                                                   ;;   )
                                                   (assoc data :xf2-a true)
                                                   )
                                           ;; :mult true
                                           :pool io-chan}]
                                         [{:xf   (fn [data]
                                                   (tap> {:xf2 data})
                                                   ;; (Thread/sleep 200)
                                                   ;; (when (= data {:e 2 :xf1 true})
                                                   ;;   (throw (ex-info "Error" {:data data}))
                                                   ;;   )
                                                   (assoc data :xf2-b true)
                                                   )
                                           ;; :mult true
                                           :pool cpu-chan}
                                          {:xf (fn [{:keys [push] :as data}]
                                                 (tap> {:xf3 data})
                                                 (if push
                                                   (do
                                                     (tap> {:PUSHED data})
                                                     :pushed
                                                     )
                                                   (assoc data :xf3 :processed))
                                                 )
                                           :pool cpu-chan}]]
                                        ;; {:xf (fn [data]
                                        ;;        (tap> {:xf4 data})
                                        ;;        (conj data :xf4)
                                        ;;        )}
                                        ;; {:xf (fn [data]
                                        ;;        (tap> {:xf5 data})

                                        ;;        ;; (Thread/sleep 1000)
                                        ;;        (conj data :xf5)
                                        ;;        )}
                                        ])

                      {:work work}
                      )
                extract-raw-results))
     (a/close! cpu-chan)
     (a/close! io-chan)
     ))

  )