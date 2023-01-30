(ns pipeline.util-test
  (:require
   [clojure.core.async :as a]
   [clojure.java.io :as io]
   [pipeline.core :as p]
   [pipeline.impl.default :as d]
   [pipeline.impl.instrumented :as i]
   [test.util :refer [wrap-apply-xf extract-results]]
   [pipeline.util :as u]
   [clojure.test :refer :all]))

(deftest log-test
  (testing "log-count"
    (let [log-events (atom [])
          log (fn [m] (swap! log-events conj m))
          result (->> (p/flow (d/wrapped (u/channeled (range 4))
                                         [{:xf        inc
                                           :log-count (u/log-count log "xf1 msg" 1)}
                                          {:xf        inc
                                           :log-count (u/log-count log "xf2 msg" 2)
                                           }])
                              (d/tasks 1)
                              {:work   (partial d/work (wrap-apply-xf d/apply-xf))})
                      extract-results)]
      (is (= (update result :result sort) {:result [2 3 4 5]}))

      (is (= (set [["xf1 msg" :count 0]
                   ["xf2 msg" :count 0]
                   ["xf1 msg" :count 1]
                   ["xf1 msg" :count 2]
                   ["xf2 msg" :count 2]
                   ["xf1 msg" :count 3]]) (set @log-events)))))
  (testing "log-period logs less with higher interval"
    (let [log-events (atom [])
          log (fn [m] (swap! log-events conj m))
          log-events-1 (do
                         (->> (p/flow (d/wrapped (u/channeled (range 10000))
                                                 [{:xf         inc
                                                   :log-period (u/log-period log "xf1 msg" 40)}])
                                      (d/tasks 1)
                                      ;; {:work   (partial w/thread (wrap-apply-xf w/apply-xf))
                                      ;;  :queue? w/queue?}
                                      )
                              extract-results)
                         @log-events)
          _ (reset! log-events [])
          log-events-2 (do
                         (->> (p/flow (d/wrapped (u/channeled (range 10000))
                                                 [{:xf         inc
                                                   :log-period (u/log-period log "xf1 msg" 10)}])
                                      (d/tasks 1)
                                      ;; {:work   (partial w/thread (wrap-apply-xf w/apply-xf))
                                      ;;  :queue? w/queue?}
                                      )
                              extract-results)
                         @log-events)]
      (is (<= (count log-events-1) (count log-events-2))))))

(deftest channeled-test
  (testing "channeled takes a channel, collection, BufferedReader or a
  function (returning any of these) and returns a channel with the the input"
    (let [source (a/chan)
          c (u/channeled source)]
      (a/go
        (dotimes [i 5]
          (a/>! source i))
        (a/close! source))
      (is (=  (a/<!! (a/into [] c)) [0 1 2 3 4])))
    (let [source (a/chan)
          c (u/channeled source 3)]
      (a/go
        (dotimes [i 5]
          (a/>! source i))
        (a/close! source))
      (is (=  (a/<!! (a/into [] c)) [0 1 2])))
    (let [source (a/chan)
          c (u/channeled (fn [] source) 3)]
      (a/go
        (dotimes [i 5]
          (a/>! source i))
        (a/close! source))
      (is (=  (a/<!! (a/into [] c)) [0 1 2])))

    (let [source (range 6)
          c (u/channeled source)]
      (is (= (a/<!! (a/into [] c)) [0 1 2 3 4 5])))
    (let [source (range 6)
          c (u/channeled source 3)]
      (is (= (a/<!! (a/into [] c)) [0 1 2])))
    (let [source (fn [] (range 6))
          c (u/channeled source 3)]
      (is (= (a/<!! (a/into [] c)) [0 1 2])))

    (let [source (u/channeled (io/reader "resources/test.csv"))
          c (u/channeled source)]
      (is (= (a/<!! (a/into [] c)) ["foo,bar" "1,2" "3,4"])))
    (let [source (u/channeled (io/reader "resources/test.csv") 2)
          c (u/channeled source)]
      (is (= ["foo,bar" "1,2"] (a/<!! (a/into [] c)))))
    (let [source (u/channeled (fn [] (io/reader "resources/test.csv")) 2)
          c (u/channeled source)]
      (is (= ["foo,bar" "1,2"] (a/<!! (a/into [] c)))))))

(deftest csv-xf-test
  (testing "Sourcing csv file"
    (let [source (u/channeled (io/reader "resources/test.csv")) ]
      (is (= [{:foo :bar
               :bar "2"}
              {:foo :bar
               :bar "4"}] (->> (p/flow (d/wrapped source
                                                  [{:xf (u/csv-xf 1000 source)}
                                                   {:xf #(assoc % :foo :bar)}])
                                       (d/tasks 1))
                               extract-results
                               :result
                               (sort-by :bar)))))))

(deftest combine-xfs
  (testing "Combining list xfs"
    (let [combined-xf (u/combine-xfs [{:xf inc}
                                      {:xf inc}])]
      (is (= 2 ((->> combined-xf first :xf) 0)))

      (let [combined-xf (u/combine-xfs [{:xf inc}
                                        {:xf inc :mult true}
                                        {:xf inc}])]
        (is (= 2 (count combined-xf) ))
        (is (= 2 ((->> combined-xf first :xf) 0)))
        (is (= 1 ((->> combined-xf second :xf) 0)))))))