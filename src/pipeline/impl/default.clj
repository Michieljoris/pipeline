(ns pipeline.impl.default
  "Runs tasks in threads, when :mult flag is set to true on xf expects multiple
   results. Catches any errors and assigns them to :data."
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [pipeline.specs :as specs]))

(defn wrapped
  "Expects a channel as source, wraps elements in a map each bundled with
   pipeline, when pipeline is a function it'll be called on every source element
   and should return a pipeline (list of maps each with a transforming function
   under the :xf key)."
  [source pipeline]
  (let [pipeline-fn (if (fn? pipeline) pipeline (constantly pipeline))]
    (a/pipe source (a/chan 1 (map #(hash-map :data % :pipeline (pipeline-fn %)))))))

(defn apply-xf
  "Actually calls the xf function on data and updates pipe to the next one.
   Returns channel with (possilbe) multiple results. Catches any errors and
   assigns them to the :data key."
  [{:keys [data pipeline] :as x}]
  (let [datas (try
                (let [{:keys [xf mult]} (first pipeline)
                      result (xf data)]
                  (if mult
                    (if (seq result) result [nil])
                    [result]))
                (catch Throwable t [t]))
        x' (assoc x :pipeline (rest pipeline))]
    (a/to-chan! (map #(assoc x' :data %) datas))))

(defn queue?
  "Decide on queueing for further processing. "
  [{:keys [pipeline data]}]
  (and (seq pipeline) (some? data)
       (not (instance? Throwable data))))

(defn work
  "Receives wrapped data as x, should call apply-xf on x asynchronously and then
   done, and return a channel with results."
  ([{:keys [data pipeline] :as x} done]
   (let  [{:keys [xf async]} (first pipeline)]
     (if async
       (let [result (a/chan 1 (map #(merge x {:data %
                                              :pipeline (rest pipeline)})))
             ;;TODO: somehow use apply-xf here? So mult and try-catch kick in?
             cb #(a/go (a/>! result %)
                       (a/close! result))]
         (xf data cb)
         (done)
         result)
       (work apply-xf x done))))
  ([apply-xf x done]
   (let [result (a/chan)]
     (a/thread (a/pipe (apply-xf x) result) (done))
     result)))

(def task-count (atom 0))

(defn inc-task-count
  [tasks]
  (when (a/offer! tasks :t)
    (swap! task-count inc)))

(defn dec-task-count
  [tasks]
  (let [[old new] (swap-vals! task-count
                              #(cond-> % (pos? %) dec))]
    (when (< new old)
      (a/go (a/<! tasks)))))

(defn tasks
   "Returns tasks channel"
  ([] (tasks 0))
  ([initial-task-count] (tasks initial-task-count 1000))
  ([initial-task-count max-task-count]
   (let [tasks (a/chan max-task-count)]
     (reset! task-count 0)
     (dotimes [_ initial-task-count] (inc-task-count tasks))
     tasks)))

(s/fdef wrapped
  :args (s/cat :source ::specs/source
               :pipeline ::specs/pipeline))

(s/fdef queue?
  :args (s/cat :x ::specs/x :data ::specs/data))

(s/fdef work
  :args (s/alt
         :arity-2  (s/cat :x ::specs/x
                          :done fn?)
         :arity-3  (s/cat :apply-xf (s/? ::specs/apply-xf)
                          :x ::specs/x
                          :done fn?))
  :ret ::specs/x)

(s/fdef inc-task-count
  :args (s/cat :tasks ::specs/chan))

(s/fdef dec-task-count
  :args (s/cat :tasks ::specs/chan))

(s/fdef tasks
  :args (s/alt :arity-1 (s/cat :initial-task-count number?)
               :arity-2 (s/cat :initial-task-count number?
                               :max-task-count number?))
  :ret ::specs/chan)

(stest/instrument
 `[wrapped tasks inc-task-count dec-task-count])