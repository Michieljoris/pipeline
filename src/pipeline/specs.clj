(ns pipeline.specs
  (:require [clojure.spec.alpha :as s]))

(s/def ::chan #(instance? clojure.core.async.impl.channels.ManyToManyChannel %))

(s/def ::source ::chan)

(s/def ::xf fn?)
(s/def ::pool ::chan)
(s/def ::mult boolean?)
(s/def ::xf-map (s/or :xf-map (s/keys :req-un [::xf ::pool]
                                      :opt-un [::mult])
                      :pipeline ::pipeline))
(s/def ::pipeline (s/or :fn fn?
                        :coll (s/coll-of ::xf-map)))
(s/def ::data (s/nilable any?))
(s/def ::x (s/keys :req-un [::pipeline ::data]))