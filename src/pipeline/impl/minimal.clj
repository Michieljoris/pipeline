(ns pipeline.impl.minimal
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as s]))

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
  (let [tasks (a/chan task-count)]
    (dotimes [_ task-count] (a/offer! tasks :t))
    tasks))

;;TODO: finish specs
(s/def ::chan #(instance? clojure.core.async.impl.channels.ManyToManyChannel %))
(s/def ::xf fn?)
(s/def ::xf (s/keys :req-un [::xf]))
(s/def ::pipeline (s/and (s/coll-of ::xf) seq))
(s/def ::source ::chan)

(s/fdef work
  :args (s/cat  :x ::x
                :done fn?)
  :ret ::x)
