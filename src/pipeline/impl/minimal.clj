(ns pipeline.impl.minimal
  (:require [clojure.core.async :as a]))

(defn wrapped
  "Wraps and bundles source elements with the pipeline."
  [source pipeline]
  (a/pipe source (a/chan 1 (map #(hash-map :data % :pipeline pipeline)))))

(defn queue?
  "Decide on queueing for further processing."
  [{:keys [pipeline data]}]
  (and (seq pipeline) (some? data)))

(defn thread
  "Minimal implementation. Receives wrapped data, calls xf with it, then done, and
   returns a channel with result."
  [{:keys [pipeline] :as x} done]
  (a/thread
    (let [x' ((-> pipeline first :xf) x)]
      (done)
      x')))
