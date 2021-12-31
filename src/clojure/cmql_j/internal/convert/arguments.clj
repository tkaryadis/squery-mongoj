(ns cmql-j.internal.convert.arguments
  (:require cmql-core.operators.operators
            [cmql-core.internal.convert.common :refer [single-maps]]
            [cmql-core.internal.convert.commands :refer
             [get-pipeline-options cmql-pipeline->mql-pipeline args->query-updateOperators-options cmql-map->mql-map split-db-namespace]]
            [cmql-j.internal.convert.options :refer [convert-options]]
            [cmql-j.driver.document :refer [clj->j-doc]])
  (:import (java.util Arrays)))


(defn jp-f [& args]
  (let [args (single-maps args #{})
        [pipeline args] (get-pipeline-options args #{})
        pipeline (cmql-pipeline->mql-pipeline pipeline)
        pipeline-map {:pipeline pipeline}
        pipeline-map (cmql-map->mql-map pipeline-map)
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