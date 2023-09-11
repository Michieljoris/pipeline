(ns core-test
  (:require
   [clojure.core.async :as a]
   [pipeline.core :as p]
   [test.util :refer [=tap wrap-apply-xf extract-results
                      extract-raw-results]]
   [pipeline.impl.default :as d]
   [pipeline.impl.minimal :as m]
   [pipeline.impl.instrumented :as i]
   [pipeline.util :as u]
   [clojure.test :refer :all]))

(defn wrap-and-number [source pipeline]
  (let [input (d/wrapped source pipeline)
        numbered (a/chan 1 (map-indexed (fn [i x] (assoc x :i i))))]
    (a/pipe input numbered)))

(deftest flow-test
  (testing "Simple pipeline"
    (is (= (->> (d/tasks 1)
                (p/flow (wrap-and-number
                         (u/channeled (range 5))
                         [{:xf inc}
                          {:xf inc}]))
                extract-raw-results)
           {:result [{:pipeline '()
                      :data 2
                      :i    0}
                     {:pipeline '()
                      :data 3
                      :i    1}
                     {:pipeline '()
                      :data 4
                      :i    2}
                     {:pipeline '()
                      :data 5
                      :i    3}
                     {:pipeline '()
                      :data 6
                      :i    4}]})))

  (testing "Returning nil from xf stops further processing "
    (is (= (->> (p/flow (wrap-and-number
                         (u/channeled (range 5))
                         [{:xf #(when (even? %) %)}
                          {:xf inc}])
                        (m/tasks 1))
                extract-raw-results)
           {:result     [{:pipeline '()
                          :data 1
                          :i    0}
                         {:pipeline '()
                          :data 3
                          :i    2}
                         {:pipeline '()
                          :data 5
                          :i    4}]
            :nil-result [{:pipeline (list {:xf clojure.core/inc})
                          :data nil
                          :i    1}
                         {:pipeline (list {:xf clojure.core/inc})
                          :data nil
                          :i    3}]})))

  (testing "Return multiple results from xf and process each"
    (is (= (->> (p/flow (wrap-and-number
                         (u/channeled (range 2))
                         [{:xf   #(vector (+ 100 %) (+ 200 %))
                           :mult true}
                          {:xf inc}])
                        (d/tasks 1))
                extract-raw-results
                :result
                (sort-by :data))
           '[{:pipeline ()
              :i        0
              :data     101}
             {:pipeline ()
              :i        1
              :data     102}
             {:pipeline ()
              :i        0
              :data     201}
             {:pipeline ()
              :i        1
              :data     202}])))

  (testing "Exceptions in xf are caught and assigned to data key"
    (let [{:keys [result error]}
          (let [{:keys [result error]}
                (->> (p/flow (wrap-and-number
                              (u/channeled (range 2))
                              [{:xf   #(vector (+ 100 %) (+ 200 %))
                                :mult true}
                               {:xf (fn [data] (if (= data 100)
                                                 (throw (ex-info "we don't like data being 100"
                                                                 {:data data}))
                                                 data))}
                               {:xf inc}])
                        (d/tasks 1))
                extract-raw-results)]
       {:result result :error error})]

      (is (= (sort-by :data result) [{:pipeline '()
                                      :data 102
                                      :i    1}
                                     {:pipeline '()
                                      :data 201
                                      :i    0}
                                     {:pipeline '()
                                      :data 202
                                      :i    1}]))
      (is (= (count error) 1))
      (is (= (first (:pipeline (first error))) {:xf clojure.core/inc}))
      (is (= (ex-message (:data (first error))) "we don't like data being 100"))
      (is (= (ex-data (:data (first error))) {:data 100}))))

  (testing "Using same tasks for two sources"
    (let [tasks (d/tasks 1)
          out1 (p/flow (d/wrapped (u/channeled (range 5))
                                  [{:xf inc}
                                   {:xf inc}])
                       tasks)
          out2 (p/flow (d/wrapped (u/channeled (map (partial + 10) (range 5)))
                                  [{:xf inc}
                                   {:xf inc}])
                       tasks)]
      (is (= [2 3 4 5 6] (sort (:result (extract-results out1)))))
      (is (= [12 13 14 15 16] (sort (:result (extract-results out2)))))))

  (testing "Apply different pipe to each source element"
    (is (=
         (sort [2 21 4 23 6])
         (->> (p/flow (d/wrapped (u/channeled (range 5))
                                 (fn [s]
                                   (if (even? s)
                                     [{:xf inc}
                                      {:xf inc}]
                                     [{:xf (partial + 10)}
                                      {:xf (partial + 10)}])))
                      (d/tasks 1))
              extract-results
              :result
              sort))))

  (testing "Change pipe mid processing while still using wrapped/apply-xf"
    (let [pipe1 [{:xf #(conj % :xf1)}
                 {:xf #(conj % :xf2)}]
          pipe2 [{:xf #(conj % :xf3)}
                 {:xf #(conj % :xf4)}]
          apply-xf (fn [x]
                     (let [c (d/apply-xf x)
                           result (a/chan 1 (map (fn [{:keys [data] :as x}]
                                                   (cond-> x
                                                     (and (even? (first data))
                                                          (< (count data) 3)) (assoc :pipeline pipe2)))))]

                       (a/pipe c result)))]
      (is (= (->> (p/flow (d/wrapped (u/channeled (map vector (range 5))) pipe1)
                          (d/tasks 1)
                          {:work   (partial d/work apply-xf)})
                  extract-results
                  :result
                  (sort-by first))
             [[0 :xf1 :xf3 :xf4]
              [1 :xf1 :xf2]
              [2 :xf1 :xf3 :xf4]
              [3 :xf1 :xf2]
              [4 :xf1 :xf3 :xf4]]))))

  (testing "Change pipe mid processing by supplying custom apply-xf"
    (let [pipe1 [{:xf #(conj % :xf1) :pipe 1}
                 {:xf #(conj % :xf2) :pipe 1}]
          pipe2 [{:xf #(conj % :xf3) :pipe 2}
                 {:xf #(conj % :xf4) :pipe 2}]
          apply-xf (fn [{:keys [data pipeline] :as x} result]
                     (let [data' ((-> pipeline first :xf) data)]
                       (-> (assoc x :data data')
                           (assoc :pipeline (if (and (even? (first data))
                                                     (< (count data) 2))
                                              pipe2
                                              (rest pipeline)))
                           vector
                           (->> (a/onto-chan! result)))))]
      (is (= (->> (p/flow (d/wrapped (u/channeled (map vector (range 5))) pipe1)
                          (d/tasks 1)
                          {:work   (fn [x done]
                                     (let [result (a/chan)]
                                       (a/thread (apply-xf x result)
                                                 (done))
                                       result))})
                  extract-results
                  :result
                  (sort-by first))
             [[0 :xf1 :xf3 :xf4]
              [1 :xf1 :xf2]
              [2 :xf1 :xf3 :xf4]
              [3 :xf1 :xf2]
              [4 :xf1 :xf3 :xf4]]))))

  (testing "Don't wrap data"
    (let [apply-xf (fn [x]
                     (let [{:keys [pipeline] :as x'} ((->> x :pipeline first :xf) x) ]
                       (a/to-chan! [(assoc x' :pipeline (rest pipeline))])))]
      (is (= '([0 :xf1 :xf2] [1 :xf1 :xf2] [2 :xf1 :xf2])
             (->> (p/flow (d/wrapped (u/channeled (map vector (range 3)))
                                     [{:xf #(update % :data conj :xf1)}
                                      {:xf #(update % :data conj :xf2)}])

                          (d/tasks 1)
                          {:work   (partial d/work apply-xf)})
                  extract-results
                  :result
                  (sort-by first))))))

  (testing "Async xf"
    (is (= (->>
            (d/tasks 1)
            (p/flow (m/wrapped (u/channeled (map vector (range 3)))
                               [{:xf    (fn [data cb]
                                          (cb (conj data :xf1)))
                                 :async true}
                                {:xf #(conj % :xf2)}]))
            extract-raw-results
            :result
            (sort-by (comp first :data)))
           [{:pipeline ()
             :data     [0 :xf1 :xf2]}
            {:pipeline ()
             :data     [1 :xf1 :xf2]}
            {:pipeline ()
             :data     [2 :xf1 :xf2]}]))))