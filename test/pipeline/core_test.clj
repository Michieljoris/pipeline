(ns core
  (:require
   [clojure.core.async :as a]
   [clojure.java.io :as io]
   [pipeline.core :as p]
   [pipeline.stat :as stat]
   [pipeline.util :as u]
   [clojure.string :as str]
   [clojure.data.csv :as csv]
   [taoensso.timbre :as log]
   [clojure.test :refer :all]))

(let [source (u/channeled (map #(hash-map :id %) (range 5)))
      pipe (p/as-pipe [{:xf #(assoc % :step-1 true)}
                       {:xf #(assoc % :step-2 true)}])
      worker (p/worker 1)
      out (p/flow source pipe worker)
      {:keys [result error nil-result]} (u/as-promises out)]
  (map #(select-keys % [:data :status]) @result)
  (sort-by :id (map :data @result))
  ;; @error
  ;; @nil-result
  )

(->> (p/flow (u/channeled (map #(hash-map :id %) (range 5)))
             (p/as-pipe [{:xf #(assoc % :step-1 :applied)}
                         {:xf #(assoc % :step-2 :applied)}])
             (p/worker 1))
     u/as-promises
     :result deref
     (map :data)
     )


(let [start-time   (stat/now)
      source (u/channeled (map #(hash-map :id %) (range 5)) 2)
      source (u/channeled (io/reader "resources/test.csv") 3)
      row->map (u/csv-xf source 1000)
      xfs [{:xf row->map
            :log-count (u/log-count tap> "Processed first xf" 20)}
           {:xf #(assoc % :step-1 true)}
           {:xf #(assoc % :step-2 true)}]
      thread-count 10
      wrapper (fn [update-x {:keys [pipe data] :as x}]
                ((:log-count pipe #(do)))
                (-> (update x :transforms (fnil conj []) data)
                    update-x))
      halt (a/chan)
      thread-hook (fn [thread-i] (tap> {:thread-i thread-i}))
      thread-hook #(u/block-on-pred % (atom 5) > halt 1000)
      {:keys [queue]} (p/threads thread-count (count xfs) {:wrapper wrapper
                                                           :thread-hook thread-hook
                                                           :halt halt})
      out (p/flow source (p/as-pipe xfs) queue)]

  (doseq [[status p] (u/out->promises out)]
    (future (tap> {status    (if (keyword? @p) @p @p)
                   :duration (/ (- (stat/now) start-time) 1000.0)})))
  )