(ns pipeline.protocol)

(defprotocol Threads
  (inc-thread-count [this])
  (dec-thread-count [this])
  (start-thread [this x thread])
  (stop-all [this])
  (flow [this in pipe] [this in pipe opts]))
