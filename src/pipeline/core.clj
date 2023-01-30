(ns pipeline.core
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [pipeline.impl.default :as d]
            [pipeline.util :as u]))

(defn flow
  "Flow source through pipeline using tasks to optionally supplied out. When
   close? is true closes out when in closes (and processing is finished).

   The work function receives two arguments, x and done. The function should
   return as fast as possible (so do any actual work in a thread, go block or
   put to result channel in a callback) and return a channel with zero or more
   results, which should close once all results are put on the channel. Once
   work is finished done, a no arg function, should be called so the task can be
   released. Tasks should be channel with filled buffer sized to desired maximum
   concurrency/parallelism task count.

   Returns out channel "
  ([source tasks] (flow source tasks nil))
  ([source tasks {:keys [out close? queue? work]
                  :or   {close? true     out  (a/chan)
                         queue? d/queue? work d/work}}]
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
     out)))

(s/def ::chan #(instance? clojure.core.async.impl.channels.ManyToManyChannel %))
(s/def ::close? boolean?)
(s/def ::out ::chan)
(s/def ::queue? fn?)
(s/def ::work fn?)
(s/def ::source ::chan)
(s/def ::flow-opts (s/keys :opt-un [:close? ::out ::queue? ::work]))

(s/fdef flow
  :args (s/cat  :source ::chan
                :tasks ::chan
                :opts ::flow-opts)
  :ret ::chan)

;; (stest/instrument
;;  `[flow])