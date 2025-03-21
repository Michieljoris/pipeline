(ns pipeline.core2
  (:require
   [clojure.core.async :as a]
   [clojure.string :as str]
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

(defn as-output
  [source {:keys [connect ops pool]}]
  (let [grouped (reduce #(assoc %1 (:id %2) %2) nil ops)]
    (tap> {:ops grouped})
    (a/pipe source (a/chan 1 (map-indexed (fn [i d] {:index   i ;;or uuid maybe better? max size is only about 2 billion
                                                     :data    d
                                                     :op {:f identity
                                                          :connect connect
                                                          :pool pool}
                                                     :ops     (reduce #(assoc %1 (:id %2) %2) nil ops)}))))))



(def hoarder-atom (atom nil))

;; (add-watch hoarder-atom :some-key (fn [k a old new]
;;                                      (tap> {:add-watch {:old old :new new}})
;;                                     ))

(defn update-h
  [m k ops fut]
  (merge m {k fut
            :ops ops}))

(defn update-hoarder
  [hoarder grouped-ops x-id op-id fut]
  (reduce-kv (fn [h in ops]
               (let [ops? (or (= in op-id)
                              (and (coll? in)
                                   (contains? (set in) op-id)))]

                 (cond-> h
                   ops? (update-in [x-id in] update-h op-id ops fut))))
             hoarder
             grouped-ops))

(defn resolve-multi-in
  [])



        ;; when out is nil:
        ;; - put a done x on the channel
        ;; when v is throwable or nil or when spill and spillable, it's empty:
        ;; - cancel all the futures for x.id and remove the entries [0 <any key that takes out>]
        ;; - put a done x on the channel


        ;; [old new]  (when op-id (let [[old new] (swap-vals! hoarder-atom update-hoarder ops x-id op-id fut)]
        ;;                          (tap> {:x-id x-id :op-id op-id :old old :new new})
        ;;                          [old new]))


(defn exec
  "Actually calls the xf function on data and updates pipe to the next one.
   Returns channel with (possilbe) multiple, splllt results. Catches any errors
   and assigns them to the :data key."
  [{:keys [index data ops]
    {:keys [f spill connect] :as op} :op
    :as x}]
 (tap> {:exec x})
  ;; check if for any ops the other inputs have already been resolved, if so,
  ;; if there's no other ops waiting for the result of (f data )  just return a closed channel
  (let [fut      (future (try (f data) (catch Throwable t t))) ;; we might want to cancel it
        new-data @fut ;; NOTE: deref with timeout here?
        next-op  (get ops connect)]
    (if (or (instance? Throwable data)
            (nil? data)
            (nil? connect)
            (nil? next-op)
            (and spill (coll? new-data) (empty? new-data)))
      [(assoc x :data new-data :done true)]
      (let [next-x (assoc x :op next-op)]
        ;; new output channel to put on the queue for flow to process the new x maps
        (if (and spill (coll? new-data))
          (mapv #(assoc next-x :data %) new-data)
          [(assoc next-x :data new-data)])))))

(defn thread
  [id chan]
  (let [close (a/chan)]
    (a/thread
      (tap> {id "started-work"})
      (loop []
        (let [[x _] (a/alts!! [chan close])]
          (when-let [[x output] x]
            (a/onto-chan! output (exec x))
            (recur))))
      (tap> {id "work done"}))
    #(a/close! close)))

(defn queue
   "Receives wrapped data as x, should call run on x asynchronously and return
   a channel with will receive results and then close."
  [{:keys [op] :as x}]
  (let [output (a/chan)]
    (a/go (a/>! (:pool op) [x output]))
    output)
  ;; (letfn [(next-output [{:keys [pool] :as x}]
  ;;           (let [output (a/chan)]
  ;;             (a/go (a/>! pool [x output]))
  ;;             output))]
  ;;   (a/merge (map #(next-output (assoc x :op %)) op)))
  )

(defn flow
  ([source] (flow source nil))
  ([sources {:keys [out close?]
            :or   {close? true  out (a/chan)}}]
   (a/go-loop [outputs sources
               i 0] ;;[source xf1-output xf2-output etc], where each vomit x's.
     (when (< i 10)
       (if (seq outputs)
         (let [[x output] (a/alts! outputs :priority true)] ;;move-things along using priority
           (tap> {:flow x})
           (recur (if (nil? x) ;; output has been closed, it's been exhausted
                    (remove #(= output %) outputs)
                    (if-not (:done x)
                      (conj outputs (queue x))
                      (do (a/>! out x)
                          outputs)))
                  (inc i)))
         (when close? (a/close! out)))))
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
                   (map #(dissoc % :ops))
                   )})


  ;; (defn wrap-and-number [source ops]
  ;;   (let [input (as-input source :property  ops)
  ;;         numbered (a/chan 1 (map-indexed (fn [i x] (assoc x :index i))))]
  ;;     [(a/pipe input numbered)]))

(declare threadpool)


(let [{cpu-chan :chan} (threadpool :cpu 1)
      {io-chan :chan}  (threadpool :io 2)
      ops              [ {:id :step-1
                          :f (fn [data]
                               (tap> {:step-1 data})
                               ;; (map #(conj data %) (range 2))
                               ;; []


                              (conj data :first-op)
                              )
                         ;; only ever one return value from a function!!!
                         ;; :in :property
                          :connect :p1
                          ;; :connect      [:io-1 :io-2]
                         ;; but we can 'spill' this result, if it's spillable, a collection
                         ;; :spill    true
                         :pool  io-chan}
                        {:id :io-1
                         :f  (fn io-1 [property]
                                 ;; (tap> {:io-1 data})
                                 (Thread/sleep 20)
                                 (conj property :io-1)
                                 )
                         :connect :process-io

                         ;;TODO: maybe make it a map with out as the key for each op, which
                         ;;will guarantee uniqueness of out handle
                         ;; :out  :io-1
                         :pool io-chan}
                        {:id :io-2
                         :f  (fn io-2  [property]
                                 ;; (tap> {:io-2 data})
                                 (Thread/sleep 50)
                                 (conj property :io-2)
                                 )
                         
                         :connect :process-io
                         :pool io-chan}

                        {:id :p1
                         :f  (fn p1  [data]
                              ;; (tap> {:xfc data})
                              (conj data :single-in1))

                         :pool io-chan}
                        ;; {:f   (fn xfb2  [data]
                        ;;          (tap> {:xfc data})
                        ;;          (conj data :single-in2))

                        ;;  :in   :io-1
                        ;;  :pool io-chan}

                        {:id :process-io
                         :f    (fn process-io  [io-1 io-2]
                                 (tap> {:io-collector {:io-1 io-1 :io-2 io-2}})
                                 {:io-collector {:io-1 io-1 :io-2 io-2}})
                         :pool io-chan}
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
                        ]
      source (u/channeled [[0]
                                        ; {:e 2}
                           ;; {:e 3}
                           ;; [:e4]
                           ;; [:e5]
                           ;; [:e6]
                           ])]
  
    (future
      (tap> (->> [(as-output source {:connect :step-1 :ops ops :pool cpu-chan})]
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

(future)
 )
