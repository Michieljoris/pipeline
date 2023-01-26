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
   [pipeline.impl.instrumented :as i]
   [pipeline.impl.default :as d])
  )


;; TODO update tests
;; TODO finish specs
;; TODO: finish readme

;; TODO: clean up dev

;; Setting thread count
;; TODO: Adjust number of threads on the fly!!!!
;; TODO: auto adjust thread count to max throughput, minimal threads, then set to 80% for example
;; TODO: auto adjust thread count to x % cpu load

;; Testing for real
 ;; TODO: test with real pipeline, such as hot-companies....
 ;; TODO: benchmark and instrument!!!!
 ;; TODO: inspect all current saigo pipelines, and document, and see if they can use this
 ;; TODO: add throttler

;; Stats
;; ------- test/showcase functionality
;; TODO: log estimate on likely duration of job
;; TODO add stats examples


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
      group-by-result-type))

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

(def mx-bean (java.lang.management.ManagementFactory/getThreadMXBean))

(def os-bean (java.lang.management.ManagementFactory/getOperatingSystemMXBean))
(doseq [method (into [] (.getDeclaredMethods (.getClass os-bean)))]
  (tap> method)

  )

(def run-time (java.lang.Runtime/getRuntime))
(.getCpuLoad os-bean)

(.availableProcessors run-time)

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

  (a/thread
    (tap> :start)
    ;; (tap> {:timeout (a/<!! (a/timeout 50000))})
    (Thread/sleep 1000)
    (tap> :end)
    )

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



  (ll-thread-count 40 0.16)

  (optimal-thread-count 1 100 100)

  (ll-through-put 2 1)

  (/ (.maxMemory run-time) 1000000.0)

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

(time (test/rand-work 100 0))
(try-future

 (let [halt            (stat/init-stats 60)
       start-time   (stat/now)
       ;; _ (reset! wrapped/task-ratios [])
       ;; _ (reset! wrapped/cpu-time-a {:last-time 0 :collect []})
       tasks (d/tasks 200)
       _ (def tasks tasks)
       xfs [{:xf (fn [data]
                   ;; (Thread/sleep 100)

                   (test/rand-work 200 0)
                   ;; (tap> {:done data})
                   ;; (Thread/sleep 1000)
                   ;; (/ 1 0)

                   ;; (tap> {:xf data} )
                   (tap> (.getCpuLoad os-bean))
                   (inc data))

             :log-count (u/log-count tap> "hello from xf1", 10)
             :i 0
             }
            ;; {:xf inc
            ;;  :i 1}
            ;; {:xf (fn [data]
            ;;        (Thread/sleep 500)
            ;;        (test/rand-work 100 0)
            ;;        ;; (tap> {:done data})
            ;;        ;; (Thread/sleep 1000)
            ;;        ;; (/ 1 0)

            ;;        ;; (tap> {:xf2 data} )
            ;;        data
            ;;        )
            ;;  }

            ;; {:xf (fn [data]
            ;;        [(inc data) (inc data)]
            ;;        ;; (inc data)
            ;;        )
            ;;  :mult true}
            ;; {:xf inc}
            ;; {:xf inc}
            ]
       source (m/wrapped (u/channeled (range 10)) xfs)
       ;; source' (a/chan 1 (map #(assoc % :queued (stat/now))))
       ;; out (a/chan 1 (map (fn [x]
       ;;                      (stat/add-stat :in-system (- (stat/now) (:queued x))) x)))
       ]
   ;; (a/pipe source source')
   (tap> (->
          (p/flow source (m/tasks 10)
                  ;; (:tasks tasks)
                  ;; {;; :out (i/out)
                  ;;  ;; :work   (d/work

                  ;;  ;;          ;; (partial i/apply-xf :i)
                  ;;  ;;          )
                  ;;  }
                  )
          (extract-raw-results)
          ;; :result
          ;; count
          ))

   ;; (tap> {:stats {:xf-0 (stat/stats :xf-0)
   ;;                :xf-1 (stat/stats :xf-1)
   ;;                :in-system (stat/stats :in-system)
   ;;                :wait (stat/stats :wait)
   ;;                :xf (stat/stats :xf)}
   ;;        :duration (/ (- (stat/now) start-time) 1000.0)})

   (a/close! halt)
   )

 )

  (try-future

   (time
    (let [halt            (stat/init-stats 60)
          start-time   (stat/now)
          ;; _ (reset! wrapped/task-ratios [])
          ;; _ (reset! wrapped/cpu-time-a {:last-time 0 :collect []})
          tasks (d/tasks 2)
          _ (def tasks tasks)
          xfs [{:xf (fn [data]
                      (Thread/sleep 100)

                      ;; (test/rand-work 200 0)
                      ;; (tap> {:done data})
                      ;; (Thread/sleep 1000)
                      ;; (/ 1 0)

                      ;; (tap> {:xf data} )

                      (inc data))
                :log-count (u/log-count tap> "hello from xf1", 10)
                :i 0
                }
               ;; {:xf inc
               ;;  :i 1}
               ;; {:xf (fn [data]
               ;;        (Thread/sleep 500)
               ;;        (test/rand-work 100 0)
               ;;        ;; (tap> {:done data})
               ;;        ;; (Thread/sleep 1000)
               ;;        ;; (/ 1 0)

               ;;        ;; (tap> {:xf2 data} )
               ;;        data
               ;;        )
               ;;  }

               ;; {:xf (fn [data]
               ;;        [(inc data) (inc data)]
               ;;        ;; (inc data)
               ;;        )
               ;;  :mult true}
               ;; {:xf inc}
               ;; {:xf inc}
               ]
          source (d/wrapped (u/channeled (range 1)) xfs)
          source' (a/chan 1 (map #(assoc % :queued (stat/now))))
          out (a/chan 1 (map (fn [x]
                               (stat/add-stat :in-system (- (stat/now) (:queued x))) x)))
          ]
      (a/pipe source source')
      (tap> (->
             (p/flow source' (:tasks tasks)
                     { :out out
                      :queue? d/queue?
                      :work   (partial d/thread (wrap-apply-xf d/apply-xf))})
             (extract-raw-results)
             ;; :result
             ;; count
             ))

      (tap> {:stats {:xf-0 (stat/stats :xf-0)
                     ;; :xf-1 (stat/stats :xf-1)
                     :in-system (stat/stats :in-system)
                     :queued (stat/stats :queued)
                     }
             :duration (/ (- (stat/now) start-time) 1000.0)})

      (a/close! halt)
      )
          )
   )

  (future (stop #(d/dec-task-count tasks)))

  (future (tap> (d/inc-task-count tasks)))
  (future (tap> (d/dec-task-count tasks)))

   (future
    (let [start-time   (stat/now)
          source (u/channeled (map #(hash-map :id %) (range 5)) 2)
          source (u/channeled (io/reader "resources/test.csv") 3)
          row->map (u/csv-xf 1000 source)
          xfs [{:xf row->map
                :log-count (u/log-count tap> "Processed first xf" 20)}
               {:xf #(assoc % :step-1 true)}
               {:xf #(assoc % :step-2 true)}]
          thread-count 10
          ;; wrapper (fn [{:keys [pipe data] :as x}]
          ;;           ((:log-count pipe #(do)))
          ;;           (-> (update x :transforms (fnil conj []) data)
          ;;               p/update-x))
          halt (a/chan)
          thread-hook (fn [thread-i] (tap> {:thread-i thread-i}))
          thread-hook #(u/block-on-pred % (atom 5) > halt 1000)
          worker (p/worker thread-count { ;; :update-x wrapper
                                         :thread-hook thread-hook
                                         :halt halt})
          out (p/flow source (p/as-pipe xfs) worker)]

      (doseq [[status p] (u/as-promises out)]
        (future (tap> {status    (if (keyword? @p) @p @p)
                       :duration (/ (- (stat/now) start-time) 1000.0)})))
      )))

(-> (with-meta {:a 1} {:some :meta})
    (assoc :b 2)
    (dissoc :a)
    meta
    )


;; DONE: log every so many seconds, but only if there's something to log
 ;; DONE: implement xf csv
 ;;   "Converts rows from a CSV file with an initial header row into a
 ;;   lazy seq of maps with the header row keys (as keywords). The 0-arg
 ;;   version returns a transducer."
 ;; DONE: what to do if (:count xfs) is 0? -> spec fails
;; DONE: how about the results from multiple datasets?
;; Each are written to their one out channel as returned from the flow fn
 ;; DONE replace all stat calls with macros that can be noop when env var is set?!?!
 ;; DONE log-count: "A transducer which logs a count of records it's seen with msg every n records."
 ;; DONE ex-handler???
 ;; DONE see if pipe can be controlled with just the one cpu utilization parameter.
 ;; DONE: test with sleep and work combination
 ;; DONE: instrument threads and submitter
 ;; DONE error handling
 ;; DONE see if thread-pool can implemented with an thread count atom
 ;; DONE see if thread pool size can be adjusted on the fly
 ;; DONE close channels ->> when thread-count is 0 for a while? Count number of results, taking splits into account??


;; (defn reset-stats []
;;   (swap! stats-atom (fn [stat-maps]
;;                       (reduce-kv (fn [acc data-point {:keys [bucket-size]}]
;;                                    (assoc acc data-point {:data {}
;;                                                           :bucket-size {}}))
;;                                  {}
;;                                  stat-maps))))


;; (u/assert-spec ::p/source (io/reader "readme.org"))

;; (let [s (u/channeled (io/reader "readme.org") nil 3)]
;;   (a/go-loop []
;;    (let [r (a/<! s)]
;;      (tap> {:r r})
;;      (when r (recur))
;;        )

;;       )

;;   )


(comment
  (def halt (a/chan))
  (stat/periodically #(tap> :hello) :one-second halt)
  (a/close! halt))

(defn pre-xf [{:keys [data xfs] :as x}]
  (tap> {:pre-xf x})
  ((:log-count (first xfs) #(do)))
  (-> (update x :transforms (fnil conj []) data)
      (stat/mark :xf-start-time))
  )

(defn xf-stats [{:keys [xfs xf-start-time xf-end-time] :as x}]
  (let [step        (-> xfs first :step) ;;TODO :step is not added to xf
        xf-i        (keyword (str "xf-" step))
        xf-duration (- xf-end-time xf-start-time)]
    (stat/add-stat :xf xf-duration)
    (stat/add-stat xf-i xf-duration))
  x)

(defn post-xf [x]
  (tap> {:post-xf x})
  (-> (stat/mark x :xf-end-time) xf-stats))

(def xfs [{:xf (fn [data]
                ;; (tap> {:step1 data})
                (let [ret (update data :step (fnil conj []) 0)]
                  (test/rand-work 20 10)
                  [(assoc ret :split 1)
                   (assoc ret :split 2)
                   (assoc ret :split 3)]
                  ret
                  ))
           :log-count (u/log-count tap> "Processed first xf" 20)
           :mult true
           }
          {:xf (fn [data]
                ;; (tap> {:step2 data})
                (test/rand-sleep 100 50)
                (if (= 0 (:id data))
                  ;; (throw (ex-info "oops!!!!" {:some :data}))
                  ;; (/ 1 0)
                  ;; nil
                  (update data :step (fnil conj []) 1)
                  (update data :step (fnil conj []) 1)))}
          {:xf (fn [data]
                ;; (tap> {:step3 data})
                (test/rand-work 300 10)
                ;; (update data :step (fnil conj []) 2)
                ;; (tap> data)
                (update data :step (fnil conj []) 2))}
          ])

(u/combine-xfs xfs)
((:xf (first (u/combine-xfs xfs))) {:id 1})
;; (p/as-pipe xfs 10)
(u/combine-xfs xfs)

(def thread-count (atom 5))

(defn read-csv
  [file-name]
  (with-open [reader (io/reader file-name)]
    (doall
     (csv/read-csv reader))))

(defn csv->maps [csv]
  (let [columns (map keyword (first csv))
        members (rest csv)]
    (map #(zipmap columns %) members) ))

(read-csv "resources/test.csv")





(comment


    ;; (a/go-loop []
    ;;   (when-let [res (a/<! out)]
    ;;     (tap> res)
    ;;     (recur)))


  (stat/stats :xf)
  (stat/stats :xf-0)
  (stat/stats :xf-1)
  (stat/stats :xf-2)
  (stat/stats :result)
  (count (:periods (:duration (deref stat/stats-atom))))
  (keys (deref stat/stats-atom))

 (reset! thread-count 1)

 (let [
       _           (def halt (a/chan))
       _            (stat/init-stats [] 60 halt)
       _            (tap> :==================================================)
       start-time   (stat/now)
       input-size   10
       log          tap>
       log          (constantly nil)
       max-thread-count 5

       on-result (fn [update-collect x]
                   ;; (stat/add-stat :result (- (stat/now) (:queued-time x)))
                   (update-collect))
       on-error  (fn [update-collect x]
                   (update-collect))
       source    (a/to-chan! (map #(hash-map :id %) (range input-size)))
       ;; thread-hook (partial u/poll-thread-count thread-count halt)
       {:keys [queue halt]}  (p/threads max-thread-count (count xfs) nil)
       _ (def halt halt)
       pipe (p/as-pipe xfs)
       out (a/chan)
       out (p/flow source pipe out queue {:on-done (fn [] ;;all source items processed
                                                     (a/close! halt)
                                                     (tap> :done!!!!!!))})
       on-processed (fn [update-collect x status]
                      (update-collect)
                      (case status
                        :result (on-result update-collect x)
                        :error  (on-error update-collect x)
                        :nil    u/noop))
       promises (u/out->promises out on-processed)]
   (doseq [[status p] promises]
     (future (tap> {status    (if (keyword? @p) @p (count @p))
                    :duration (/ (- (stat/now) start-time) 1000.0)})))
   )

  (a/close! halt)
  )



;; async pipeline
(comment
  (time
   (let [input-size 5000
         input (map #(hash-map :id %) (range input-size))
         pipeline (a-pipeline/pipeline [{:xf (map (get-in xfs [0 :f])) :threads 4}
                                        ;; {:xf (mapcat flatten ) :threads 1}
                                        {:xf (map (get-in xfs [1 :f])) :threads 80}
                                        {:xf (map (get-in xfs [2 :f])) :threads 4}
                                        ;; {:xf (get-in xfs [2 :f]) :threads 10}
                                        ;; {:xf (map inc)}
                                        ])
         ;; pipeline (a-pipeline/pipeline [{:xf (map inc) :threads 2}
         ;;                                {:xf (map inc) :threads 2}
         ;;                                {:xf (map inc) :threads 2}
         ;;                                ;; {:xf (map inc)}
         ;;                                ])

         result (a-pipeline/flow pipeline input
                                 {:collect? true})]

     (count @result)
     ))

  )

;; (defn process-result [{:keys [data xfs thread] :as x}
;;                       monitor
;;                       {:keys [log done mult] :as opts}]
;;   (log {:process-result x})

;;   (let [
;;         {to-queue true
;;          to-done false} (->> (mult data)
;;                                  (map #(assoc x :data %))
;;                                  (map #(assoc % :status (get-status % opts)))
;;                                  (group-by #(= (:status %) :to-queue)))
;;         ]

;;     (log {:todo {:to-queue to-queue :to-done to-done} })

;;     (doseq [x to-done] (done! done x))

;;     ;;NOTE: perhaps create a channel with buffer size (count to-queue),
;;     ;;then (a/onto-chan! c to-queue) and (a/admix queue c)
;;     (go
;;       (doseq [x to-queue] (submit x xfs))
;;       ;; (swap! monitor assoc thread nil)
;;       )

;;     ))



(comment

  (let [done (chan)
        {:keys [promises streams]} (process-done done {:result :stream})]
    (def promises promises)
    (def streams streams)
    (def done done)
    (future  (tap> {:deref (deref (:result promises))}))
    (go (tap> {:??? (a/<! (:result streams))})
        (tap> {:??? (a/<! (:result streams))})
        )
    )

  (go (tap> (a/>! done {:status :result :foo 2})))

  (a/close! done)


  {:promises promises
   :streams streams})

          ;; (stat/add-stat :duration (- (stat/now) (:queued-time x)))
          ;; (tap> {:result-n result-count
          ;;        ;; :result (select-keys result [:index :data :status])
          ;;        })



;; (macroexpand '(submit! {:foo 1} {:opts 1}))

;;NOTE: minimum nr of queues is (max  (map count <all of xfs vectors used>))

            ;; :or {max-xfs-count 32 ;;
            ;;      cpu-thread-count 4 ;; set to cpu count
            ;;      io-thread-count 32} ;; low number will throttle io heavy pipelines

(defn log-xf [x] (tap> {:log-xf x}) x)
(defn apply-mult [{:keys [mult] :as x}]
  (mult x))
(comment
  {nil   (mult {:data nil})
   []    (mult {:data []})
   1     (mult {:data 1})
   [1]   (mult {:data [1]})
   [1 2] (mult {:data [1 2]})
   [nil] (mult {:data [nil]})}

  (seq [])
  (mult {:data []})



  (let [c (chan 1 (comp (map mult)
                        cat
                        (map calc-status)
                        ))]
    (go
      (a/>! c {:data 1})      ;;{:data [1]}
      (a/>! c {:data []})     ;; {:data [nil]}
      (a/>! c  {:data nil})   ;; {:data [nil]}
      (a/>! c  {:data [1 2]}) ;; {:data [1 2]}
      (a/>! c  {:data [nil]}) ;; {:data [nil]}
      (a/>! c  {:data {:some :data}})
      )

    (go
      (while true
        (tap> (a/<! c)))




      )
    ))

           ;;NOTE: needed?
    ;; (check-monitor @monitor monitor done)

;; (defn abort?
;;   "Decide if processing of data should come to an end. Returned value will be
;;    assigned to x as received by done channel under status key. When
;;    returning nil status key value will be either :result or :error"
;;   [data]
;;   (when (or (and (sequential? data)
;;                  (empty? data)) ;;xf returning empty vector/list or nil aborts any further work
;;             (nil? data)) :aborted))

;; (defn mult
;;   "HAS to return x with a value for the :data key that can be mapped over."
;;   [data] ;;NOTE: this doesn't work for huge count of data, might run out of memory
;;   (if (sequential? data)
;;     (if (empty? data) [nil] data)
;;     (list data)))


;; (defmacro submit [x xfs done start data]
;;   `(let [status# (cond (instance? Throwable ~data) :error
;;                        (empty? ~xfs)               :result
;;                        (nil? ~data)                :done
;;                        :else                       :queue)]
;;      (if (= status# :queue)
;;        (do  (~start)
;;             (a/>! (-> ~xfs first :queue) (assoc ~x :status status#)))
;;        (a/>! ~done (assoc ~x :status status#)))))

         ;; _ (add-watch monitor :ref (fn [_ _ old new]
         ;;                             (tap> {:monitor [old new]})

         ;;                             ))

(do
  (defmacro foo []
    `(if (queue? ~'pipe ~'data)
       (do
         (~'check-in)
         (a/>!~'queue ~'x-to-queue))
       (a/>! ~'out ~'x-to-queue)))
  (macroexpand '(foo)))

;; (when (some-> x update-x (enqueue queues)) (recur))

(defn default-wrapper
  "Expects the update-x fn to be called on x and the result to be returned. To be
   used to hook into pre and post (xf data), eg. for stats or debugging."
  [update-x x]
  (update-x x))

;; (defn assert-spec [spec data]
;;   (assert (s/valid? spec data) (s/explain-str spec data)))

(defn split-by
  "Takes a predicate, a source channel, and a map of channels. Out channel is
   selected looking in the outs map for the result of applying predicate to
   values. Outputs to channel under :default key if not found. The outs will
   close after the source channel has closed."
  [outs p ch]
  (let [{:keys [default] :as outs'}
        (update outs :default  #(or % (a/chan (a/dropping-buffer 1))))]
    (a/go-loop []
      (let [v (a/<! ch)]
        (if (some? v)
          (when (a/>! (get outs' (p v) default) v)
            (recur))
          (doseq [out (vals outs')] (a/close! out)))))
    outs'))


;; (defn apply-xf
;;   "Default implementation. Calls the pipeline's xf function on wrapped data and
;;    updates pipeline."
;;   [{:keys [data pipeline] :as x} result]
;;   (let [x' (merge x {:data     ((-> pipeline first :xf) data)
;;                      :pipeline (rest pipeline)})]
;;     (a/go (a/>! result x') (a/close! result))))