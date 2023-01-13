(ns core
  (:require
   [clojure.core.async :as a]
   [clojure.java.io :as io]
   [pipeline.core :as p]
   [pipeline.stat :as stat]
   [pipeline.util :as u]
   [pipeline.mult :as mult]
   [pipeline.catch-ex :as catch-ex]
   [clojure.string :as str]
   [clojure.data.csv :as csv]
   [taoensso.timbre :as log]
   [clojure.test :refer :all]))

;; TEST:
;;- adjust thread count on the fly
;;- DONE combine-xfs
;;- DONE use version of apply-xf fn  that doesn't wrap x

;;- DONE change pipe mid job for an x.
;;- DONE set different pipe for every source element
;;- DONE use same worker for more than 1 job, having different pipes
;;- DONE channeled (csv, map, channel, n)
;;- DONE basic
;;- DONE throw exception
;;- DONE log-period and log-count
;;- DONE mult

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

(defn =tap [a b]
  (tap> {:result a :expected b})
  (= a b))

(comment

 (future
   (let [thread-atom (atom 1)
         pred (fn [thread-i] (=  thread-i 1)) ;;TODO how to check which thread was used for an xf and x?
         worker (p/worker 1  {:hook #(u/block-on-pred % pred (a/chan) 1000)})]
     (->> (p/flow (u/channeled (map vector (range 5)))
                  (p/as-pipe [{:xf inc}
                              {:xf inc}])
                  worker)
          extract-results
          tap>))

   )


  (future
    (tap> :===============================)
    (tap> (->> (p/flow (u/channeled (range 5))
                       (p/as-pipe [{:xf inc}
                                   {:xf inc}])
                       (p/worker 1))
               extract-results))
    #_(let [log-events (atom [])
            log        (fn [m] (swap! log-events conj m))]
      (tap> (->> (p/flow (u/channeled (range 10000))
                         (p/as-pipe [{:xf (fn [data]
                                            (inc data))
                                      :log-period (u/log-period log "xf1 msg" 40)}
                                     ;; {:xf inc
                                     ;;  :log-count (u/log-count log "xf2 msg" 2)
                                     ;;  }
                                     ])
                         (p/worker 1 {:apply-xf (apply-xf-fn catch-ex/apply-xf)
                                      :enqueue catch-ex/enqueue}))
                 extract-results))
      (tap> @log-events)
      )

    )
  (future
    (tap> :=====================)
    (tap> (let [{:keys [result error]}
                (->> (p/flow (u/channeled (range 2))
                             (p/as-pipe [{:xf   #(vector (+ 100 %) (+ 200 %))
                                          :mult true}
                                         {:xf (fn [data] (if (= data 100)
                                                           (throw (ex-info "we don't like data being 100"
                                                                           {:data data}))
                                                           data))}
                                         ;; {:xf inc}
                                         ])
                             (p/worker 1 {:apply-xf (apply-xf-fn mult/apply-xf)
                                          :enqueue  mult/enqueue
                                          }))
                     extract-raw-results)]
            result
            ;; (= result [{:pipe nil
            ;;             :data 201
            ;;             :i 0}
            ;;            {:pipe nil
            ;;             :data 202
            ;;             :i 1}
            ;;            {:pipe nil
            ;;             :data 102
            ;;             :i 1}])
            ;; (= (count error) 1)
            ;; (= (:pipe (first error)) {:xf clojure.core/inc
            ;;                           :i 2})
            
            ;; (= (ex-message (:data (first error))) "we don't like data being 100")
            ;; (= (ex-data (:data (first error))) {:data 100})
            ;; error
            )

          )
       ;; {:result [{:pipe nil
       ;;            :data 201
       ;;            :i 0}
       ;;           {:pipe nil
       ;;            :data 101
       ;;            :i 0}
       ;;           {:pipe nil
       ;;            :data 202
       ;;            :i 1}
       ;;           {:pipe nil
       ;;            :data 102
       ;;            :i 1}]}

    ;; {:result [{:pipe nil
    ;;             :data 1
    ;;             :i 0}
    ;;            {:pipe nil
    ;;             :data 1
    ;;             :i 0}]}
    ;; (tap> (->> (p/flow (u/channeled (range 5))
    ;;                    (p/as-pipe [{:xf #(when (even? %) %)}
    ;;                                {:xf inc-fn}])

    ;;                    (p/worker 1))
    ;;            extract-results
    ;;            ))
    ))


(deftest flow-test
  (testing "Simple pipe"
    (is (= (->> (p/flow (u/channeled (range 5))
                        (p/as-pipe [{:xf inc}
                                    {:xf inc}])
                        (p/worker 1 {:apply-xf (apply-xf-fn p/apply-xf)}))
                extract-raw-results)
           {:result [{:pipe nil
                      :data 2
                      :i    0}
                     {:pipe nil
                      :data 3
                      :i    1}
                     {:pipe nil
                      :data 4
                      :i    2}
                     {:pipe nil
                      :data 5
                      :i    3}
                     {:pipe nil
                      :data 6
                      :i    4}]})))

  (testing "Returning nil from xf stops further processing "
    (is (= (->> (p/flow (u/channeled (range 5))
                        (p/as-pipe [{:xf #(when (even? %) %)}
                                    {:xf inc}])
                        (p/worker 1  {:apply-xf (apply-xf-fn p/apply-xf)}))
                extract-raw-results)
           {:result     [{:pipe nil
                          :data 1
                          :i    0}
                         {:pipe nil
                          :data 3
                          :i    2}
                         {:pipe nil
                          :data 5
                          :i    4}]
            :nil-result [{:pipe {:xf clojure.core/inc
                                 :i  1}
                          :data nil
                          :i    1}
                         {:pipe {:xf clojure.core/inc
                                 :i  1}
                          :data nil
                          :i    3}]})))

  (testing "Return multiple results from xf and process each"
    (is (= (->> (p/flow (u/channeled (range 2))
                        (p/as-pipe [{:xf   #(vector (+ 100 %) (+ 200 %))
                                     :mult true}
                                    {:xf inc}])
                        (p/worker 1 {:apply-xf (apply-xf-fn mult/apply-xf)
                                     :enqueue  mult/enqueue}))
                extract-raw-results)
           {:result [{:pipe nil
                      :data 201
                      :i    0}
                     {:pipe nil
                      :data 101
                      :i    0}
                     {:pipe nil
                      :data 202
                      :i    1}
                     {:pipe nil
                      :data 102
                      :i    1}]})))

  (testing "Exceptions in xf are caught and assigned to data key"
    (let [{:keys [result error]}
          (->> (p/flow (u/channeled (range 2))
                       (p/as-pipe [{:xf   #(vector (+ 100 %) (+ 200 %))
                                    :mult true}
                                   {:xf (fn [data] (if (= data 100)
                                                     (throw (ex-info "we don't like data being 100"
                                                                     {:data data}))
                                                     data))}
                                   {:xf inc}])
                       (p/worker 1 {:apply-xf (apply-xf-fn mult/apply-xf)
                                    :enqueue  mult/enqueue
                                    }))
               extract-raw-results)]

      (is (= result [{:pipe nil
                      :data 201
                      :i    0}
                     {:pipe nil
                      :data 202
                      :i    1}
                     {:pipe nil
                      :data 102
                      :i    1}]))
      (is (= (count error) 1))
      (is (= (:pipe (first error)) {:xf clojure.core/inc
                                    :i  2}))
      (is (= (ex-message (:data (first error))) "we don't like data being 100"))
      (is (= (ex-data (:data (first error))) {:data 100}))))

  (testing "Using same worker for two sources"
    (let [worker (p/worker 1)
          out1 (p/flow (u/channeled (range 5))
                       (p/as-pipe [{:xf inc}
                                   {:xf inc}])
                       worker)
          out2 (p/flow (u/channeled (map (partial + 10) (range 5)))
                       (p/as-pipe [{:xf inc}
                                   {:xf inc}])
                       worker)]
      (is (= [2 3 4 5 6] (sort (:result (extract-results out1)))))
      (is (= [12 13 14 15 16] (sort (:result (extract-results out2)))))))

  (testing "Apply different pipe to each source element"
    (is (=
         (sort [2 21 4 23 6])
         (->> (p/flow (u/channeled (range 5))
                      (fn [s]
                        (if (even? s)
                          (p/as-pipe [{:xf inc}
                                      {:xf inc}])
                          (p/as-pipe [{:xf (partial + 10)}
                                      {:xf (partial + 10)}])))
                      (p/worker 1))
              extract-results
              :result
              sort))))
  (testing "Change pipe mid processing"
    (let [pipe1 (p/as-pipe [{:xf #(conj % :xf1)}
                            {:xf #(conj % :xf2)}])
          pipe2 (p/as-pipe [{:xf #(conj % :xf3)}
                            {:xf #(conj % :xf4)}])
          apply-xf (fn [x]
                     (let [{:keys [data] :as x'} (p/apply-xf x)]
                       (cond-> x'
                         (and (even? (first data))
                              (< (count data) 3)) (assoc :pipe pipe2))))]
      (is (= [[0 :xf1 :xf3 :xf4]
              [1 :xf1 :xf2]
              [2 :xf1 :xf3 :xf4]
              [3 :xf1 :xf2]
              [4 :xf1 :xf3 :xf4]]
             (->> (p/flow (u/channeled (map vector (range 5)))
                          pipe1
                          (p/worker 1  {:apply-xf apply-xf}))
                  extract-results
                  :result
                  (sort-by first))))))
  (testing "Don't wrap data"
    (let [apply-xf (fn [x]
                     (let [{:keys [pipe] :as x'} ((->> x :pipe :xf) x) ]
                       (assoc x' :pipe (:next pipe))))]
      (is (= '([0 :xf1 :xf2] [1 :xf1 :xf2] [2 :xf1 :xf2])
             (->> (p/flow (u/channeled (map vector (range 3)))
                          (p/as-pipe [{:xf #(update % :data conj :xf1)}
                                      {:xf #(update % :data conj :xf2)}])
                          (p/worker 1  {:apply-xf apply-xf}))
                  extract-results
                  :result
                  (sort-by first)
                  )))))

  )

(deftest log-test
  (testing "log-count"
    (let [log-events (atom [])
          log (fn [m] (swap! log-events conj m))
          result (->> (p/flow (u/channeled (range 4))
                              (p/as-pipe [{:xf inc
                                           :log-count (u/log-count log "xf1 msg" 1)}
                                          {:xf inc
                                           :log-count (u/log-count log "xf2 msg" 2)
                                           }])
                              (p/worker 1 {:apply-xf (apply-xf-fn catch-ex/apply-xf)
                                           :enqueue catch-ex/enqueue}))
                      extract-results)]
      (is (= result {:result [2 3 4 5]}))

      (is (= (set @log-events) (set [["xf1 msg" :count 0]
                                     ["xf2 msg" :count 0]
                                     ["xf1 msg" :count 1]
                                     ["xf1 msg" :count 2]
                                     ["xf2 msg" :count 2]
                                     ["xf1 msg" :count 3]])))))

  (testing "log-period logs less with higher interval"
    (let [log-events (atom [])
          log (fn [m] (swap! log-events conj m))
          log-events-1 (do
                         (->> (p/flow (u/channeled (range 10000))
                                      (p/as-pipe [{:xf inc
                                                   :log-period (u/log-period log "xf1 msg" 40)}])
                                      (p/worker 1 {:apply-xf (apply-xf-fn catch-ex/apply-xf)
                                                   :enqueue  catch-ex/enqueue}))
                              extract-results)
                         @log-events)
          _ (reset! log-events [])
          log-events-2 (do
                         (->> (p/flow (u/channeled (range 10000))
                                      (p/as-pipe [{:xf inc
                                                   :log-period (u/log-period log "xf1 msg" 10)}])
                                      (p/worker 1 {:apply-xf (apply-xf-fn catch-ex/apply-xf)
                                                   :enqueue  catch-ex/enqueue}))
                              extract-results)
                         @log-events)]
      (is (<= (count log-events-1) (count log-events-2))))))

(deftest channeled-test
  (testing "channeled takes a channel, collection, BufferedReader or a
  function (returning any of these) and returns a channel with the the input"
    (let [source (a/chan)
          c (u/channeled source)]
      (a/go
        (dotimes [i 5]
          (a/>! source i))
        (a/close! source))
      (is (=  (a/<!! (a/into [] c)) [0 1 2 3 4])))
    (let [source (a/chan)
          c (u/channeled source 3)]
      (a/go
        (dotimes [i 5]
          (a/>! source i))
        (a/close! source))
      (is (=  (a/<!! (a/into [] c)) [0 1 2])))
    (let [source (a/chan)
          c (u/channeled (fn [] source) 3)]
      (a/go
        (dotimes [i 5]
          (a/>! source i))
        (a/close! source))
      (is (=  (a/<!! (a/into [] c)) [0 1 2])))

    (let [source (range 6)
          c (u/channeled source)]
      (is (= (a/<!! (a/into [] c)) [0 1 2 3 4 5])))
    (let [source (range 6)
          c (u/channeled source 3)]
      (is (= (a/<!! (a/into [] c)) [0 1 2])))
    (let [source (fn [] (range 6))
          c (u/channeled source 3)]
      (is (= (a/<!! (a/into [] c)) [0 1 2])))

    (let [source (u/channeled (io/reader "resources/test.csv"))
          c (u/channeled source)]
      (is (= (a/<!! (a/into [] c)) ["foo,bar" "1,2" "3,4"])))
    (let [source (u/channeled (io/reader "resources/test.csv") 2)
          c (u/channeled source)]
      (is (= ["foo,bar" "1,2"] (a/<!! (a/into [] c)))))
    (let [source (u/channeled (fn [] (io/reader "resources/test.csv")) 2)
          c (u/channeled source)]
      (is (= ["foo,bar" "1,2"] (a/<!! (a/into [] c)))))))

(deftest csv-xf-test
  (testing "Sourcing csv file"
    (let [source (u/channeled (io/reader "resources/test.csv")) ]
      (is (= [{:foo :bar
               :bar "2"}
              {:foo :bar
               :bar "4"}] (->> (p/flow source
                                       (p/as-pipe [{:xf (u/csv-xf 1000 source)}
                                                   {:xf #(assoc % :foo :bar)}])
                                       (p/worker 1))
                               extract-results
                               :result
                               (sort-by :bar)))))))

(deftest combine-xfs
  (testing "Combining list xfs"
    (let [combined-xf (u/combine-xfs [{:xf inc}
                                      {:xf inc}])]
      (is (= 2 ((->> combined-xf first :xf) 0)))

      (let [combined-xf (u/combine-xfs [{:xf inc}
                                        {:xf inc :mult true}
                                        {:xf inc}])]
        (is (= 2 (count combined-xf) ))
        (is (= 2 ((->> combined-xf first :xf) 0)))
        (is (= 1 ((->> combined-xf second :xf) 0)))))))


(comment
 (future
   )


  (let [source                            (u/channeled (map #(hash-map :id %) (range 5)))
        pipe                              (p/as-pipe [{:xf #(assoc % :step-1 true)}
                         {:xf #(assoc % :step-2 true)}])
        worker                            (p/worker 1)
        out                               (p/flow source pipe worker)
        {:keys [result error nil-result]} (u/as-promises out)]
    (map #(select-keys % [:data :status]) @result)
    (sort-by :id (map :data @result))
    ;; @error
    ;; @nil-result
    )






  (let [start-time   (stat/now)
        source (u/channeled (map #(hash-map :id %) (range 5)) 2)
        source (u/channeled (io/reader "resources/test.csv") 3)
        row->map (u/csv-xf source 1000)
        xfs [{:xf row->map
              :log-count (u/log-count tap> "Processed first xf" 20)}
             {:xf #(assoc % :step-1 true)}
             {:xf #(assoc % :step-2 true)}]
        thread-count 10
        wrapper (fn [update-x {:keys [pipe data] :as x}]
                  ((:log-count pipe #(do)))
                  (-> (update x :transforms (fnil conj []) data)
                      update-x))
        halt (a/chan)
        thread-hook (fn [thread-i] (tap> {:thread-i thread-i}))
        thread-hook #(u/block-on-pred % (atom 5) > halt 1000)
        {:keys [queue]} (p/threads thread-count (count xfs) {:wrapper wrapper
                                                             :thread-hook thread-hook
                                                             :halt halt})
        out (p/flow source (p/as-pipe xfs) queue)]

    (doseq [[status p] (u/out->promises out)]
      (future (tap> {status    (if (keyword? @p) @p @p)
                     :duration (/ (- (stat/now) start-time) 1000.0)})))
    )

  (do
    (defn ms->duration [ms]
      (let [hours (quot ms (* 60 60 1000))
            minutes (- (quot ms (* 60  1000)) (* hours 60))
            seconds (- (quot ms 1000) (* minutes 60) (* hours 60 60))]
        (cond-> ""
          (> hours 1) (str hours " hours and ")
          (= hours 1) (str hours " hour and ")
          (> minutes 1) (str minutes " minutes and ")
          (= minutes 1) (str minutes " minute and ")
          (> seconds 1) (str seconds " seconds")
          (= seconds 1) (str seconds " second"))

        (str hours "h:" minutes "m:" seconds "s"))
      )
    (ms->duration (+ (* 62 60 1000) 1000))


    )
  )

(comment
  (let [source (a/chan)
        c (u/channeled source)]
    (a/go
      (dotimes [i 5]
        (a/>! source i))
      (a/close! source))
    (future
      (tap> {:?? (a/<!! (a/into [] c))})
      ;; (tap> {:result-c (a/<!! r)})
      )
    )

  (let [source (range 3)
        c (u/channeled source)]
    (future
      (tap> {:?? (a/<!! (a/into [] c))})
      ;; (tap> {:result-c (a/<!! r)})
      )
    )
  (let [source (u/channeled (io/reader "resources/test.csv") 3)
        c (u/channeled source)]
    (future
      (tap> {:?? (a/<!! (a/into [] c))})
      ;; (tap> {:result-c (a/<!! r)})
      )
    )
  )