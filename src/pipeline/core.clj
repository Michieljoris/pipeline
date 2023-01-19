(ns pipeline.core
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [pipeline.util :as u]))

(defn flow
  "Flow source through pipeline using tasks to optionally supplied out. When
   close? is true closes out when in closes (and processing is finished).

   The work function receives two arguments, x and done. The function should
   return as fast as possible (so do any actual work in a thread, go block or
   put to result channel in a callback) and return a channel with zero or more
   results, which should close once all results are put on the channel. Once
   work is finished done, a no arg function, should be called so the task can be
   released.

   Returns out channel "
  [source tasks {:keys [out close? queue? work]
                 :or   {close? true out (a/chan)}}]
  (let [monitor (atom 0)
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
            (if (queue? x)
              (do
                (check-in)
                (a/<! tasks) ;;wait for task to be available
                (recur (conj inputs (work x #(a/>!! tasks :t)))))
              (do (a/>! out x)
                  (recur inputs)))))))
    out))

(defn tasks
  "Returns map with stateful tasks data, and two update functions."
  ([] (tasks 1000))
  ([max-task-count]
   (let [tasks (a/chan max-task-count)
         task-count (atom 0)]
     {:tasks          tasks
      :task-count     task-count
      :inc-task-count #(when (a/offer! tasks :t)
                           (swap! task-count inc))
      :dec-task-count (fn []
                          (let [[old new] (swap-vals! task-count
                                                      #(cond-> % (pos? %) dec))]
                            (when (< new old)
                              (a/go (a/<! tasks)))))})))

;;TODO: finish specs
(s/def ::xf fn?)
(s/def ::apply-xf fn?)
(s/def ::enqueue fn?)
(s/def ::hook  fn?)
(s/def ::task-count pos-int?)
(s/def ::queue-count pos-int?)
(s/def ::chan #(instance? clojure.core.async.impl.channels.ManyToManyChannel %))
(s/def ::halt ::chan)
(s/def ::xf (s/keys :req-un [::xf]))
(s/def ::next (s/nilable ::pipe))
(s/def ::pipe (s/keys :req-un [::xf ::next]))
(s/def ::xfs (s/and (s/coll-of ::xf) seq))
(s/def ::worker-opts (s/keys :opt-un [::queue-count ::hook ::halt ::apply-xf ::enqueue]))
(s/def ::source (s/or :buffered-reader u/buffered-reader?  :coll coll? :channel u/channel? :fn fn?))

(s/fdef worker
  :args (s/cat  :task-count ::task-count
                :opts ::worker-opts)
  :ret ::chan)

(s/fdef as-pipe
  :args (s/cat  :xfs ::xfs
                ::ofsett pos-int?)
  :ret ::pipe)

(s/fdef flow
  :args (s/cat  :in ::chan
                :pipe ::pipe
                :worker ::chan
                :opts ::flow-opts)
  :ret ::chan)

;; (stest/instrument
;;  `[worker
;;    as-pipe
;;    flow])

   ;; (u/assert-spec ::worker-args {:task-count task-count :opts opts} )

  ;; (s/alt :nullary (s/cat)
  ;;              :unary (s/cat :config ::config))