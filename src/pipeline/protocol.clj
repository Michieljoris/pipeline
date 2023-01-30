(ns pipeline.protocol
  (:require
   [clojure.core.async :as a]
   [pipeline.impl.default :as d]
   [pipeline.core :as p]
   [pipeline.impl.minimal :as m]
   [pipeline.impl.instrumented :as i]))

(defprotocol Pipeline
  (source [this source pipeline])
  (queue? [this x])
  (work [this x done] [this apply-xf x done])
  (flow [this tasks]))

(defprotocol Tasks
  (tasks [this])
  (inc-task-count [this])
  (dec-task-count [this]))

(defrecord Minimal [source pipeline out close?]
  Pipeline
  (wrap [this] (m/wrapped source pipeline))
  (queue? [this x] (m/queue? x))
  (work [this x done] (m/work x done))
  (flow [this tasks]
    (let [source (wrap this)
          monitor (atom 0)
          check-in #(swap! monitor inc)
          check-out #(when (and (zero? (swap! monitor dec)) close?)
                       (a/close! out))]

      (check-in)

      (a/go-loop [inputs #{source}]
        (when (seq inputs)
          (let [[x input] (a/alts! (vec inputs))]
            (if (nil? x)
              (do
                (check-out)
                (recur (disj inputs input))) ;;end of input
              (if (queue? this x)
                (do
                  (check-in)
                  (a/<! tasks) ;;wait for task to be available
                  (recur (conj inputs (work x #(a/>!! tasks :t)))))
                (do (a/>! out x)
                    (recur inputs)))))))
      out)))

(defrecord SimpleTasks [task-count]
  Tasks
  (tasks [this] (m/tasks task-count)))

(defn simple-tasks [task-count]
  (map->SimpleTasks {:task-count task-count}))

(defn minimal [source pipeline]
  (map->Minimal {:source source :pipeline pipeline})
  )