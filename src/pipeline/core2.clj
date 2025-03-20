(ns pipeline.core2
  (:require
   [clojure.core.async :as a]
   [pipeline.util :as u]
   [pipeline.impl.default :as d]
   ;; [com.qantashotels.property-content-importer-task.stats :as stats]
   )
  )

;; (defn create
;;   "Expects a channel as source, wraps elements in a map each bundled with
;;    pipeline, when pipeline is a function it'll be called on every source element
;;    and should return a pipeline (list of maps each with a transforming function
;;    under the :xf key)."
;;   [source pipeline]
;;   (let [pipeline-fn (if (fn? pipeline) pipeline (constantly pipeline))]
;;     (a/pipe source (a/chan 1 (map #(hash-map :data % :pipeline (pipeline-fn %)))))))

(defn as-input
  [source in ops]
  (let [grouped (group-by :in ops)]
    (a/pipe source (a/chan 1 (map #(hash-map :data %
                                             :op (get grouped in)
                                             :ops (dissoc grouped in)))))))

(defn exec-op
  "Actually calls the xf function on data and updates pipe to the next one.
   Returns channel with (possilbe) multiple, splllt results. Catches any errors
   and assigns them to the :data key."
  [{:keys [data ops]
    {:keys [f spill out]} :op :as x}]
   ;; (tap> {:exec-op x})
  (let [ret (try (f data) (catch Throwable t t))
        spillable? (coll? ret)
        next-op (get ops out)
        next-x (merge x {:data ret
                         :op   next-op
                         :done (or (instance? Throwable ret)
                                   (nil? out)
                                   (nil? next-op)
                                   (nil? ret)
                                   (and spill spillable? (empty? ret)))})]
    (a/to-chan! (if (:done next-x)
                  [next-x]
                  (if (and spill spillable?)
                    (map #(assoc next-x :data %) (:data next-x))
                    [next-x])))))

(defn work-thread
  [id chan]
  (let [close (a/chan)]
    (a/thread
      (tap> {id "started-work"})
      (loop []
        (let [[x _] (a/alts!! [chan close])]
          (when-let [[x output] x]
            (a/pipe (exec-op x) output)
            (recur))))
      (tap> {id "work done"}))
    #(a/close! close)))

(defn collect
  [inputs every?]
  (let [n (count inputs)]
    (a/go-loop [inputs @inputs
                results []]
      (let [[x input] (a/alts! inputs)]
        (if x
          (if (and every? (< (count results) n)
                   (recur inputs (conj results x)))
            :foo ;;exec the op that wanted any ret
            )
          )

        )
      ))

  )

(defn work
   "Receives wrapped data as x, should call exec-op on x asynchronously and return
   a channel with results."
  [{:keys [op] :as x}]
  (letfn [( next-output
           [{:keys [op collect] :as x}]
           (let [{:keys [pool]} op
                 output (a/chan)]
             (a/go (a/>! pool [x output]))
             output))]
    (a/merge (map #(next-output (assoc x :op %)) op))))

(defn flow
  ([source] (flow source nil))
  ([sources {:keys [out close? work]
            :or   {close? true  out (a/chan) work work}}]
   (a/go-loop [inputs sources] ;;[source xf1-output xf2-output etc], where each vomit x's.
     (if (seq inputs)
       (let [[x input] (a/alts! inputs :priority true)] ;;move-things along using priority
          ;; (tap> {:flow x})
         (recur (if (nil? x) ;; input has been closed, it's been exhausted
                  (remove #(= input %) inputs)
                  (if-not (:done x)
                    (conj inputs (work x))
                    (do (a/>! out x)
                        inputs)))))
       (when close? (a/close! out))))
   out))


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
                   (map :data)
                   )})


  (defn wrap-and-number [source ops]
    (let [input (as-input source :property  ops)
          numbered (a/chan 1 (map-indexed (fn [i x] (assoc x :index i))))]
      [(a/pipe input numbered)]))

(declare threadpool)

(let [{cpu-chan :chan} (threadpool :cpu 1)
      {io-chan :chan}  (threadpool :io 1)
      ops [{:f (fn [data]
                 (tap> {:xf1 data})
                 (map #(conj data %) (range 2))
                 ;; []

                 )
            ;; only ever one return value from a function!!!
            :in :property
            :out      :b
            ;; but we can 'spill' this result, if it's spillable, a collection
            :spill    true
            :pool  io-chan}
           {:f   (fn xfb1 [data]
                    (tap> {:b1 data})
                    (conj data :b1)
                    )
            :in   :b
            :out :c
            :pool io-chan}
           {:f   (fn xfb2  [data]
                    (tap> {:b2 data})
                    (conj data :b2)
                    )
            :in   :b
            :out :c
            :pool io-chan}
           {:f   (fn xfb2  [data]
                    (tap> {:xfc data})
                    (conj data :c))

            :in   :c
            :every true

             ;; :in   :c
            :pool io-chan}]
      source (u/channeled [[0]
                                        ; {:e 2}
                           ;; {:e 3}
                           ;; [:e4]
                           ;; [:e5]
                           ;; [:e6]
                           ])]
  
    (future
      (tap> (->> (wrap-and-number source ops)
                 flow
                 extract-raw-results
                 ))
      (a/close! cpu-chan)
      (a/close! io-chan)
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
