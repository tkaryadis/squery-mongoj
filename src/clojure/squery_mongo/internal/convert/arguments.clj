(ns squery-mongo.internal.convert.arguments
  (:require squery-mongo-core.operators.operators
            [squery-mongo-core.internal.convert.common :refer [single-maps]]
            [squery-mongo-core.internal.convert.commands :refer
             [get-pipeline-options squery-pipeline->mql-pipeline args->query-updateOperators-options squery-map->mql-map split-db-namespace]]
            [squery-mongo.internal.convert.options :refer [convert-options]]
            [squery-mongo.driver.document :refer [clj->j-doc]])
  (:import (java.util Arrays)))


(defn jp-f [& args]
  (let [args (single-maps args #{})
        [pipeline args] (get-pipeline-options args #{})
        pipeline (squery-pipeline->mql-pipeline pipeline)
        pipeline-map {:pipeline pipeline}
        pipeline-map (squery-map->mql-map pipeline-map)
        pipeline (get pipeline-map "pipeline")
        pipeline (Arrays/asList
                   (into-array (clj->j-doc pipeline)))]
    pipeline))

(defn u-f [& args]
  (let [[query update-operators options] (args->query-updateOperators-options args #{})
        update-operators (apply (partial merge {}) update-operators)]
    (clj->j-doc update-operators)))


(defn convert-arg [arg method-name]
  (cond

    (and (map? arg) (contains? arg :__pipeline__))
    (get arg :__pipeline__)

    (and (map? arg) (contains? arg :__filter__))
    (.get (first (get arg :__filter__)) "$match")

    (and (map? arg) (contains? arg :__options__))
    (convert-options (get arg :__options__) method-name)

    (map? arg)
    (clj->j-doc arg)

    :else
    arg))