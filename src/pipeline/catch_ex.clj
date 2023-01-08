(ns pipeline.catch-ex)

(defn apply-xf
  "Calls the xf function on data and updates pipe to the next one."
  [{:keys [data pipe] :as x}]
  (merge x {:data (try ((:xf pipe) data) (catch Throwable t t))
            :pipe (:next pipe)}))
