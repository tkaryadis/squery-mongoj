(ns cmql-j.test
  (:refer-clojure :only [])
  (:use cmql-j.api
        cmql-j.client
        cmql-j.cursor
        cmql-j.document
        cmql-j.settings
        cmql.operators.operators
        cmql.operators.stages
        clojure.pprint)
  (:refer-clojure)
  (:require [clojure.core :as c]))


(init-defaults {:client (connect) :decode "clj"})

(def doc {:myarray [{:a [1 2 3] :b 2} {:a [1 2] :b [1 2]}]
          :myarray1 [{:a [1 2 3] :b 2} {:a [1 2] :b [1 2]}]})

(drop-collection :testdb.testcoll)

(insert :testdb.testcoll doc)

(create-index :testdb.testcoll (index [:myarray.a :myarray1.a]))

(c-print-all (q :testdb.testcoll
                ;(match {:myarray {"$eq" 3}})
                ))