(ns pipeline.impl.minimal
  "Minimal implementation of pipeline: constant task count, xf function receives
   wrapped data and needs to update pipeline itself."
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [pipeline.specs :as specs]))

(defn wrapped
  "Wraps and bundles source elements with the pipeline."
  [source pipeline]
  (a/pipe source (a/chan 1 (map #(hash-map :data % :pipeline pipeline)))))

(defn queue?
  "Decide on queueing for further processing. Will continue processing as long as
   there's still pipeline left"
  [x]
  (seq (:pipeline x)))

(defn work
  "Minimal implementation. Receives wrapped data, calls xf on it, then done, and
   returns a channel with result. Expects xf to update the
   pipeline (usually (rest pipeline))."
  [{:keys [pipeline] :as x} done]
  (a/thread
    (let [x' ((-> pipeline first :xf) x)]
      (done)
      x')))

(defn tasks [task-count]
  (let [c (a/chan task-count)]
    (dotimes [_ task-count] (a/offer! c :t))
    c))

(s/fdef wrapped
  :args (s/cat :source ::specs/source
               :pipeline ::specs/pipeline))

(s/fdef queue?
  :args (s/cat :x ::specs/x))

(s/fdef work
  :args (s/cat  :x ::specs/x
                :done fn?)
  :ret ::specs/x)

(s/fdef tasks
  :args (s/cat :task-count number?)
  :ret ::specs/chan)

(stest/instrument
 `[wrapped tasks])