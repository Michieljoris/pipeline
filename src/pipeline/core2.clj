(ns pipeline.core2
  (:require
   [clojure.core.async :as a]
   [pipeline.util :as u]))

;; Considerations
;; - maybe add all-ops to every x?  It is possible this way to change ops mid processing thoughs
;;- replace keywords in connect with the actual ops. saves some looking up and checking in exec fn
;;- it's possible to 'join' (async) data streams, and continue as soon as something arrives, or block till everything is there
;;  but this can be implemented 100% in 'user' space. Add an atom before splitting a data into two or more blocking io ops
;;  then execute the io in a future, add the future to the atom and deref (block) the future. Do this is for each io op
;;  All io ops should connect to an aggregator fn. For 'any' logic, continue with data passed in but future-cancel any unrealized futures in the atom
;; For 'every' logic, check if there's any unrealized futures, if so return nil and do nothing. When all futures are realized, deref them and
;; process them.

;; TODO:
;; - update readme and implementations and tests
;; - hydrate connect keyword
;; - add examples of parallel io
;; - explain logic of core.clj and how every x holds a return value and the op to be applied to it.
;;   and how a op can result in "spilt' results, each processed further with the next connect
;;   and how any op can connect zero, one or more than other other op, and also be spilled as well..
;; - update doc strings

;; ====================================================================================================

(defn exec
  "Actually calls the xf function on data and updates pipe to the next one.
   Returns channel with (possilbe) multiple, splllt results. Catches any errors
   and assigns them to the :data key."
  [{:keys [data op all-ops] :as x}]
  (let [{:keys [f spill connect]} op
        ret (try (f data) (catch Throwable t t))
        spill? (and spill (coll? ret))
        ops (mapv #(get all-ops %) connect)  ;;NOTE: maybe memoize? Or prehyydrate
        done? (or (instance? Throwable ret)
                  (nil? ret)
                  (and spill? (empty? ret))
                  (empty? ops))] ;; never empty if prehydrated
    (if done? [ret]
       (for [data (cond-> ret (not spill?) vector)
             op ops]
         (assoc x :data data :op op)))))

(defn thread
  [id chan]
  (let [close (a/chan)]
    (a/thread
      (loop []
        (let [[x _] (a/alts!! [chan close])]
          (when-let [[x output] x]
            (a/onto-chan! output (exec x))
            (recur)))))
    #(a/close! close)))

(defn queue
   "Receives wrapped data as x, should call run on x asynchronously and return
   a channel with will receive results and then close."
  [{:keys [op] :as x}]
  (let [output (a/chan)]
    (a/go (a/>! (:pool op) [x output]))
    output))

(defn flow
  ([sources] (flow sources nil))
  ([sources {:keys [out close?]
             :or   {close? true  out (a/chan)}}]
   (a/go-loop [outputs sources] ;;[source xf1-output xf2-output etc], where each vomit x's.
     (if (seq outputs)
       (let [[x output] (a/alts! outputs :priority true)] ;;move-things along using priority
         (recur (if (nil? x) ;; output has been closed, it's been exhausted
                  (remove #(= output %) outputs)
                  (if (:op x)
                    (conj outputs (queue x))
                    (do (a/>! out x)
                        outputs)))))
       (when close? (a/close! out))))
   out))

;; ====================================================================================================


(def group-by-result-type
  (comp (partial group-by (fn [{:keys [data pipe]}]
                       (cond (instance? Throwable data) :error
                             (empty? pipe)              :result
                             (nil? data)                :nil-result
                             :else                      :queue)))
        #(sort-by :i %)))

(defn extract-raw-results [out]
  {:raw-result (->> out
                   u/as-promise
                   deref
                   ; group-by-result-type
                   ;; (map #(dissoc % :all-ops))
                   )})


(declare threadpool)

(defn as-output
  [source ops op]
  (a/pipe source (a/chan 1 (map-indexed (fn [i d] {:index   i
                                                   :data    d
                                                   :op      op
                                                   :all-ops ops})))))


(tap> :====================================================================================================)
(let [{cpu-chan :chan} (threadpool :cpu 1)
      {io-chan :chan}  (threadpool :io 2)
      ops              {:step-1 {:id :step-1
                                 :f  (fn [data]
                                       (tap> {:step-1 data})
                                       ;; (map #(conj data %) (range 2))
                                       ;; []


                                       (conj data :first-op)
                                       )
                                 :connect [:io-1 :io-2]
                                 ;; but we can 'spill' this result, if it's spillable, a collection
                                 ;; :spill    true
                                 :pool    io-chan}
                        :io-1 {:id      :io-1
                               :f       (fn io-1 [property]
                                          ;; (tap> {:io-1 data})
                                          (Thread/sleep 20)
                                          (conj property :io-1)
                                          )
                               :connect [:process-io]
                               :pool    io-chan}
                        :io-2 {:id :io-2
                               :f  (fn io-2  [property]
                                     ;; (tap> {:io-2 data})
                                     (Thread/sleep 50)
                                     (conj property :io-2)
                                     )

                               :connect [:process-io]
                               :pool    io-chan}

                        :process-io {:id :process-io
                                     :f  (fn p1  [data]
                                           ;; (tap> {:xfc data})
                                           (conj data :single-in1))

                                     :pool io-chan}
                        ;; {:f   (fn xfb2  [data]
                        ;;          (tap> {:xfc data})
                        ;;          (conj data :single-in2))

                        ;;  :in   :io-1
                        ;;  :pool io-chan}

                        ;; {:id :process-io
                        ;;  :f    (fn process-io  [io-1 io-2]
                        ;;          (tap> {:io-collector {:io-1 io-1 :io-2 io-2}})
                        ;;          {:io-collector {:io-1 io-1 :io-2 io-2}})
                        ;;  :pool io-chan}
                        ;; {:in   #{:io-1 :io-2}
                        ;;  :f    (fn h1  [io-1-or-2] ;; whichever is first
                        ;;          (tap> {:io-collector {:io-1-or-2 io-1-or-2}})
                        ;;          {:io-collector {:io-1-or-2 io-1-or-2}})

                        ;;  :pool io-chan}
                        ;; {:f   (fn h2  [data-c data-d]
                        ;;         (tap> {:xfc {:data-c data-c :data-d data-d}})
                        ;;         {:xfc {:data-c data-c :data-d data-d}})
                        ;;  :in   [:c :d]
                        ;;  :every true
                        ;;  :pool io-chan}
                        }
      source (u/channeled [[0]
                                        ; {:e 2}
                           ;; {:e 3}
                           ;; [:e4]
                           ;; [:e5]
                           ;; [:e6]
                           ])]
  
    (future
     (try
       (tap> (-> (flow [(as-output source ops (get ops :step-1))])
                 extract-raw-results))
       (tap> :closing-threads)
       (a/close! cpu-chan)
       (a/close! io-chan)
       (catch Exception e (tap> {:e e}))
       )
      )
    )

(comment
  ;; (do
  ;;   (def ^{:})

  ;;   )

  )

;; ====================================================================================================

;; (defn work-thread-instrumented
;;   [id chan]
;;   (let [close (a/chan)
;;         stat (atom {:wait 0
;;                     :n 0})]
;;     (a/thread
;;       (tap> {id "started-work"})
;;       (let [start (stats/now)]
;;         (loop []
;;           (let [wait-start (stats/now)
;;                 [x _]      (a/alts!! [chan close])
;;                 wait-end (stats/now)]
;;             (swap! stat update :wait  + (- wait-end wait-start))
;;             (when-let [[x output] x]
;;               (swap! stat update :n inc)
;;               (a/pipe (apply-xf x) output)
;;               (recur))))
;;         (let [end      (stats/now)
;;               duration (- end start)
;;               waiting  (:wait @stat)]
;;           (tap> {id {:idle-procent (int (* 100.0 (/ waiting duration)))
;;                      :duration duration
;;                      :stat         @stat}}))))
;;     #(a/close! close)))

(defn threadpool
  [id n]
  (let [chan (a/chan)
        state (atom {:threads-count n})
        inc-threads (fn []
                    (swap! state (fn [{:keys [close-fns] :as s}]
                                   (update s :close-fns conj (thread (keyword (str (name id) "-") (str (count close-fns))) chan)))))
        dec-threads (fn []
                      (swap! state (fn [{:keys [close-fns] :as s}]
                                     (when (> (count close-fns) 0)
                                       (update s :close-fns (fn [close-fns]
                                                              ((last close-fns))
                                                              (butlast close-fns)))))))
        start-all (fn []
                  (swap! state (fn [{:keys [threads-count close-fns] :as s}]
                                (when (zero? (count close-fns))
                                  (assoc s :close-fns (mapv #(thread (keyword (str (name id) "-") (str %)) chan) (range threads-count)))))))]
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


;; https://puredanger.github.io/tech.puredanger.com/2010/05/30/clojure-thread-tricks/
(comment
  (do
    (def throw (atom nil))

    (defn interruptable
      [interrupt?]
      (future
        (try
          (while true
            (tap> :sleeping)
            (Thread/sleep 1000)
            (interrupt?)
            )
          (catch InterruptedException e (tap> {:e e})))
        ))
    (def fut (interruptable (fn interrupt []
                              (when @throw
                                (tap> :throwing)
                                (throw (InterruptedException. "Function interrupted..."))))))
    )
  (future-cancel fut)

  (reset! throw true)

  (do
    (def t (a/thread-call (fn [] (tap> :foo) (Thread/sleep 1000) (tap> :done))))

    ;; (.start t)
    (.interrupt t)
    )

 )


  ;; (letfn [(next-output [{:keys [pool] :as x}]
  ;;           (let [output (a/chan)]
  ;;             (a/go (a/>! pool [x output]))
  ;;             output))]
  ;;   (a/merge (map #(next-output (assoc x :op %)) op)))