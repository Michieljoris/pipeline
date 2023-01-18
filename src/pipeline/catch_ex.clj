(ns pipeline.catch-ex
  (:require [clojure.core.async :as a]))

(defn apply-xf
  "Default implementation. Calls the pipeline's xf function on wrapped data and
   updates pipeline."
  [{:keys [data pipeline] :as x} result]
  (let [x' (merge x {:data     (try ((-> pipeline first :xf) data)
                                    (catch Throwable t t))
                     :pipeline (rest pipeline)})]
    (a/go (a/>! result x') (a/close! result))))

(defn queue? [pipeline data]
  (tap> {:queue pipeline :data data})
  (and (seq pipeline) (some? data)
       (not (instance? Throwable data))))