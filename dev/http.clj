(ns http
  (:require
   [clj-http.client :as client]
   ;; [clojure.core.async :as a]
   [org.httpkit.client :as http]
   [pipeline.core :as p]
   [pipeline.impl.default :as d]
   [pipeline.impl.minimal :as m]
   [pipeline.util :as u])
  )

(def group-by-result-type
  (comp (partial group-by (fn [{:keys [data pipe]}]
                       (cond (instance? Throwable data) :error
                             (empty? pipe)              :result
                             (nil? data)                :nil-result
                             :else                      :queue)))
        #(sort-by :i %)))

(defn extract-raw-results [out]
  (-> out
      u/as-promise
      deref
      group-by-result-type
      ))


(def url "http://localhost:20018")

;; (defn request [url options]
;;   (let [result (a/chan)]
;;     (http/get url options
;;               (fn [{:keys [status headers body error] :as response}] ;; asynchronous response handling
;;                 (a/go
;;                   (a/>! result {:status status :body body :error error}))
;;                 (if error
;;                   (println "Failed, exception is " error)
;;                   (do
;;                     (tap> {:body body})
;;                     (println "Async HTTP GET: " status)))))
;;     (tap> {:done :with-call})
;;     result))

(defn clj-http [url options cb]
    (client/request {:url url
                     :method :get
                     :async? true}
                    ;; respond callback
                    (fn [response] (println "response is:" response)
                      (cb response)
                      )
                    ;; raise callback
                    (fn [exception] (println "exception message is: " (.getMessage exception))
                      (cb exception)
                      ))
    :done
    )
(def options {:timeout 2000             ; ms
              ;; :basic-auth ["user" "pass"]
              ;; :query-params {:param "value" :param2 ["value1" "value2"]}
              ;; :user-agent "User-Agent-string"
              ;; :headers {"X-Header" "Value"}
              :as :text
              })

(comment

  ;; (let [result (a/chan)]
  ;;   (http/get url options
  ;;             (fn [{:keys [status headers body error]}] ;; asynchronous response handling
  ;;               (a/go
  ;;                 (a/>! result {:status status :body body :error error}))
  ;;               (if error
  ;;                 (println "Failed, exception is " error)
  ;;                 (do
  ;;                   (tap> {:body body})
  ;;                   (println "Async HTTP GET: " status)))))

  ;;   ;; (tap> {:done :with-call})
  ;;   ;; (tap> {:result (a/<!! result)})
  ;;   (def result result)
  ;;   )


  (future
    (->> (p/flow (m/wrapped (u/channeled (range 10))
                            [{:xf (fn [data  cb
                                       ]
                                    (tap> {:data data :making :request})
                                    (clj-http url options cb)
                                    ;; (http/get url options  cb)
                                    )
                              :async true
                              }
                             ;; {:xf (fn [response]
                             ;;        (tap> {:response response})
                             ;;        (assoc response :xf2 :!!!!!!!!)
                             ;;        )}
                             ])
                 (d/tasks 100)
                 ;; {:work work}
                 )
         extract-raw-results
         ;; (take 3)
         :result
         count
         tap>))

  )