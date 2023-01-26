(ns pipeline.impl.minimal
  (:require [clojure.core.async :as a]))

(defn wrapped
  "Wraps and bundles source elements with the pipeline."
  [source pipeline]
  (a/pipe source (a/chan 1 (map #(hash-map :data % :pipeline pipeline)))))

(defn queue?
  "Decide on queueing for further processing. Will continue processing as long as
   there's still pipeline left"
  [x]
  (seq (:pipeline x)))

(defn thread
  "Minimal implementation. Receives wrapped data, calls xf on it, then done, and
   returns a channel with result. Expects xf to update the
   pipeline (usually (rest pipeline))."
  [{:keys [pipeline] :as x} done]
  (a/thread
    (let [x' ((-> pipeline first :xf) x)]
      (done)
      x')))
