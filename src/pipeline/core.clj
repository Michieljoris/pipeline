(ns pipeline.core
  (:require
   [pipeline.stat :as stat]
   [clojure.spec.alpha :as s]
   [pipeline.test :as test]
   ;; [com.theladders.async.pipeline :as a-pipeline]
   [clojure.core.async :as a :refer [chan go-loop go]]
   [taoensso.timbre :as log]))

;; TODO: merge xfs if they indicate they don't return multiple results
;; TODO: take blocking quotient into account!!!!
;; TODO: test with real pipeline.
;; TODO: benchmark!!!!
;; TODO: inspect all current saigo pipelines, and document, and see if they can use submitter
;; TODO: add throttler
;; TODO: what to do if (:count xfs) is 0?
;; TODO: expose xfs itself to xf fns, so an x can be chucked onto a different pipeline
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

;; ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

(s/def ::f fn?)
(s/def ::wrapped-f fn?)
(s/def ::mult (s/nilable boolean))
(s/def ::thread-count pos-int?)
(s/def ::queue-count pos-int?)
(s/def ::chan #(instance? clojure.core.async.impl.channels.ManyToManyChannel %))
(s/def ::halt ::chan)
(s/def ::queue ::chan)
(s/def ::queues (s/coll-of ::queue))
(s/def ::xf (s/keys :req-un [::f]
                    :opt-un [::mult]))
(s/def ::xfs (s/and (s/coll-of ::xf) seq))
(s/def ::pipeline-xf (s/keys :req-un [::wrapped-f ::queue]
                             :opt-un [::mult]))
(s/def ::pipeline-xfs (s/and (s/coll-of ::pipeline-xf) seq))
(s/def ::threads-args (s/keys :req-un [::thread-count ::queue-count]
                              :opt-un [::chan]))

(s/def ::pipeline-args (s/keys :req-un [::xfs ::queues]
                               :opt-un []))

(s/def ::flow-args (s/keys :req-un [::source ::pipeline-xfs]))

(defn assert-spec [spec data]
  (assert (s/valid? spec data) (s/explain-str spec data)))

(defn threads [thread-count queue-count halt]
  (assert-spec ::threads-args {:thread-count thread-count :queue-count queue-count :halt halt})
  (let [queues (->> (repeatedly chan) (take queue-count))
        p-queues (reverse (into (if halt [halt] []) queues))]
    (dotimes [_ thread-count]
      (a/thread
        (loop []
          (let [[x-to-process _] (a/alts!! p-queues :priority true)]
            (when (some? x-to-process)
              (let [xf (-> x-to-process :xfs first)
                    {:keys [xfs done start end data] :as x} ((:wrapped-f xf) x-to-process)]
                (go (doseq [data data]
                      (let [status (cond (instance? Throwable data) :error
                                         (empty? xfs)               :result
                                         (nil? data)                :nil
                                         :else                      :queue)
                            x' (assoc x :data  data :status status)]
                        (if (= status :queue)
                          (do  (start)
                               (a/>! (-> xfs first :queue) x'))
                          (a/>! done x'))))
                    (end)))
              (recur))))))
    queues))

(defn try-xf [{:keys [mult f]} data]
  (try
    (let [result (f data)]
      (if mult
        (if (seq result) result [nil])
        [result]))
    (catch Throwable t t)))

(defn pipeline
  ([xfs queues] (pipeline xfs queues nil))
  ([xfs queues {:keys [pre-xf post-xf] :or {pre-xf identity post-xf identity}}]
   (assert-spec ::pipeline-args {:xfs xfs :queues queues})
   (assert (>= (count queues) (count xfs))
           (str "Not enough queues (" (count queues) ") for steps in pipeline ("(count xfs) ")"))
   (map (fn [xf queue]
          (merge xf {:wrapped-f (fn [{:keys [data xfs] :as x}]
                                  (-> (pre-xf x)
                                      (assoc :data (try-xf xf data)
                                             :xfs (rest xfs))
                                      post-xf))
                     :queue     queue}))
        xfs queues)))

(defn flow
  ([source xfs] (flow source xfs {}))
  ([source xfs {:keys [on]}]
   ;; (assert-spec ::flow-args {:source source :xfs xfs})
   (let [done (chan)
         monitor (atom 0)
         wrapper {:start #(swap! monitor inc)
                  :end #(when (zero? (swap! monitor dec))
                          ((on :done (fn [])))
                          (a/close! done))
                  :xfs xfs
                  :done done}
         source-chan (chan 1 (map (fn [data]
                                    ((:start wrapper))
                                    (-> (assoc wrapper :data data)
                                        ((on :queue identity))))))
         promises {:result (promise):error (promise) :other (promise)}]

     (a/onto-chan! source-chan source)
     (a/pipe source-chan (->> xfs first :queue) false)

     (go-loop [collect nil]
       (let [{:keys [status] :as x} (a/<! done)]
         (if (some? x)
           (let [cb (on status #(constantly nil))]
             (recur (assoc collect status (cb (status collect) x))))
           (doseq [[out p] promises]
             (deliver p (or (get collect out) :done))))))
     promises)))

;;
;;
;; ----------------------------------------------------------------------------------------------------
;; ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
;; Dev
(defn log-count [msg n]
  (let [cnt (atom 0)]
    (fn []
      (let [new-cnt (dec (swap! cnt inc))]
        (when (zero? (mod new-cnt n))
          (tap> {:count new-cnt})
          (log/info msg :count new-cnt))))))

(def xfs [{:step 1
           :f (fn [data]
                ;; (tap> {:step1 data})
                (let [ret (update data :step (fnil conj []) 0)]
                  (test/rand-work 20 10)
                  [(assoc ret :split 1)
                   (assoc ret :split 2)
                   (assoc ret :split 3)]
                  ret
                  ))
           :log-count (log-count "Processed first xf" 300)
           ;; :mult true
           }
          {:step 2
           :f (fn [data]
                ;; (tap> {:step2 data})
                (test/rand-sleep 100 50)
                (if (= 0 (:id data))
                  ;; (throw (ex-info "oops!!!!" {:some :data}))
                  ;; (/ 1 0)
                  ;; nil
                  (update data :step (fnil conj []) 1)
                  (update data :step (fnil conj []) 1)))}
          {:step 3
           :f (fn [data]
                ;; (tap> {:step3 data})
                (test/rand-work 30 10)
                ;; (update data :step (fnil conj []) 2)
                ;; (tap> data)
                (update data :step (fnil conj []) 2))}
          ])


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

;; ((:f (first (combine-xfs [{:f inc} {:foo :bar :f inc}]))) 0)

(defn pre-xf [{:keys [data xfs] :as x}]
  ;; (tap> {:pre-xf x})
  ((:log-count (first xfs) #(do)))
  ;; (-> (update x :transforms (fnil conj []) data)
  ;;     (stat/mark :xf-start-time))
  x
  )

(defn xf-stats [{:keys [xfs xf-start-time xf-end-time] :as x}]
  (let [step        (-> xfs first :step) ;;TODO :step is not added to xf
        xf-i        (keyword (str "xf-" step))
        xf-duration (- xf-end-time xf-start-time)]
    (stat/add-stat :xf xf-duration)
    (stat/add-stat xf-i xf-duration))
  x)

(defn post-xf [x]
  ;; (tap> {:post-xf x})
  (-> (stat/mark x :xf-end-time) xf-stats))



(comment
  (stat/stats :xf)
  (stat/stats :xf-0)
  (stat/stats :xf-1)
  (stat/stats :xf-2)
  (stat/stats :result)
  (count (:periods (:duration (deref stat/stats-atom))))
  (keys (deref stat/stats-atom))

  (let [ _ (def halt (chan))
        _ (stat/init-stats [] 60 halt)
        _ (tap> :==================================================)
        start-time (stat/now)
        input-size      50
        log             tap>
        log             (constantly nil)
        thread-count    10
        source (map #(hash-map :id %) (range input-size))
        queues (threads thread-count (count xfs) halt)
        some-pipeline (pipeline xfs queues {
                                            :pre-xf  pre-xf
                                            ;; :post-xf post-xf
                                            })
        outs (flow source some-pipeline {:log log
                                         :on  {:result (fn [collect x]
                                                         (stat/add-stat :result (- (stat/now) (:queued-time x)))
                                                         (conj collect x))
                                               :error  (fn [collect x]
                                                         (conj collect x))
                                               :nil    nil
                                               :queue  #(assoc % :queued-time (stat/now))
                                               :done   (fn []
                                                         (a/close! halt)
                                                         (tap> :done!!!!!!))}})]
    ;; (tap> {:outs outs})
    (doseq [[out p] outs]
      (future (tap> {out (if (keyword? @p) @p (count @p))
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

(defn split-by
  "Takes a predicate, a source channel, and a map of channels. Out channel is
   selected looking in the outs map for the result of applying predicate to
   values. Outputs to channel under :default key if not found. The outs will
   close after the source channel has closed."
  [p ch outs]
  (let [{:keys [default] :as outs'}
        (update outs :default  #(or % (chan (a/dropping-buffer 1))))]
    (go-loop []
      (let [v (a/<! ch)]
        (if (some? v)
          (when (a/>! (get outs (p v) default) v)
            (recur))
          (doseq [out (vals outs')] (a/close! out)))))
    outs'))

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