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

;; TEST:
;;- basic
;;- adjust thread count
;;- demo use of wrapper
;;- throw exception
;;- change pipe mid job for an ex
;;- log-period and log-count
;;- mult
;;- channeled (csv, map, channel, n)

(def group-by-result-type
  (comp (partial group-by (fn [{:keys [data pipe]}]
                       (cond (instance? Throwable data) :error
                             (empty? pipe)              :result
                             (nil? data)                :nil-result
                             :else                      :queue)))
        #(sort-by :i %)
        ))

(defn extract-raw-results [out]
  (-> out
      u/as-promise
      deref
      group-by-result-type))

(defn extract-results [out]
(reduce-kv (fn [acc k v]
             (assoc acc k (mapv :data v)))
        {}
        (extract-raw-results out)))

(defn apply-xf []
  (let [i (atom -1)]
    (fn [x]
      (p/apply-xf
       (cond-> x
         (not (:i x)) (assoc :i (swap! i inc)) )))))

(defn =tap [a b]
  (tap> {:result a :expected b})
  (= a b))

(comment
  (future
    (tap> (->> (p/flow (u/channeled (range 5))
                       (p/as-pipe [{:xf inc}
                                   {:xf inc}])
                       (p/worker 1 {:apply-xf (apply-xf)}))
               extract-results))
    ;; (tap> (->> (p/flow (u/channeled (range 5))
    ;;                    (p/as-pipe [{:xf #(when (even? %) %)}
    ;;                                {:xf inc-fn}])

    ;;                    (p/worker 1))
    ;;            extract-results
    ;;            ))
    ))


(deftest basic-pipe
  (testing "Simple pipe"
    (is (= (->> (p/flow (u/channeled (range 5))
                        (p/as-pipe [{:xf inc}
                                    {:xf inc}])
                        (p/worker 1 {:apply-xf (apply-xf)}))
                extract-raw-results)
           {:result [{:pipe nil
                      :data 2
                      :i 0}
                     {:pipe nil
                      :data 3
                      :i 1}
                     {:pipe nil
                      :data 4
                      :i 2}
                     {:pipe nil
                      :data 5
                      :i 3}
                     {:pipe nil
                      :data 6
                      :i 4}]}))

    (testing "Returning nil from xf stops further processing "
        (is (= (->> (p/flow (u/channeled (range 5))
                            (p/as-pipe [{:xf #(when (even? %) %)}
                                        {:xf inc}])
                            (p/worker 1  {:apply-xf (apply-xf)}))
                    extract-raw-results)
               {:result [{:pipe nil
                          :data 1
                          :i 0}
                         {:pipe nil
                          :data 3
                          :i 2}
                         {:pipe nil
                          :data 5
                          :i 4}]
                :nil-result [{:pipe {:xf clojure.core/inc
                                     :i 1}
                              :data nil
                              :i 1}
                             {:pipe {:xf clojure.core/inc
                                     :i 1}
                              :data nil
                              :i 3}]})))

    )

  ;; (is (= (->> (p/flow (u/channeled (map #(hash-map :id %) (range 5)))
  ;;                     (p/as-pipe [{:xf #(assoc % :step-1 :applied)}
  ;;                                 {:xf #(assoc % :step-2 :applied)}])
  ;;                     (p/worker 1))
  ;;             u/as-promises
  ;;             :result deref
  ;;             (map :data)
  ;;             (sort-by :id))
  ;;        '({:step-2 :applied
  ;;           :step-1 :applied
  ;;           :id     0}
  ;;          {:step-2 :applied
  ;;           :step-1 :applied
  ;;           :id     1}
  ;;          {:step-2 :applied
  ;;           :step-1 :applied
  ;;           :id     2}
  ;;          {:step-2 :applied
  ;;           :step-1 :applied
  ;;           :id     3}
  ;;          {:step-2 :applied
  ;;           :step-1 :applied
  ;;           :id     4})))
  )

(comment
  (let [source                            (u/channeled (map #(hash-map :id %) (range 5)))
        pipe                              (p/as-pipe [{:xf #(assoc % :step-1 true)}
                         {:xf #(assoc % :step-2 true)}])
        worker                            (p/worker 1)
        out                               (p/flow source pipe worker)
        {:keys [result error nil-result]} (u/as-promises out)]
    (map #(select-keys % [:data :status]) @result)
    (sort-by :id (map :data @result))
    ;; @error
    ;; @nil-result
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
    ))