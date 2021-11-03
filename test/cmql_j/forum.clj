(ns cmql-j.forum
  (:refer-clojure :only [])
  (:use cmql-j.macros
        cmql-j.interop.arguments
        cmql-j.interop.mongo-collection
        cmql-j.interop.driver-core
        cmql-j.commands
        cmql-j.client
        cmql-j.cursor
        cmql-j.document
        cmql-j.settings
        cmql.operators.operators
        cmql.operators.stages
        clojure.pprint)
  (:refer-clojure)
  (:require cmql.operators.stages
            [clojure.core :as c])
  (:import (com.mongodb.client MongoClients MongoClient MongoDatabase MongoCollection)
           (com.mongodb WriteConcern)
           (com.mongodb.client.model CountOptions Filters InsertOneModel)))

(init-defaults {:client (connect) :decode "clj"})

(def db (.getDatabase @default-client "testdb"))
(def coll ^MongoCollection (.getCollection db "testcoll"))

(.drop coll)

(def doc [{:samples [{:n (date "2020-01-01T00:00:00Z")} {:n (date "2020-02-01T00:00:00Z")} {:n (date "2020-03-01T00:00:00Z")}]}
          {:samples [{:n (date "2010-01-01T00:00:00Z")} {:n (date "2011-01-01T00:00:00Z")} {:n (date "2012-01-01T00:00:00Z")}]}])

(insert :testdb.testcoll doc)

(create-index :testdb.testcoll (index [:samples.n]))

(c-print-all (q :testdb.testcoll
                { "$match" {
                            "$and" [{"samples.n" {"$gt" (date "2019-01-01T00:00:00Z")}}
                                    {"samples.n" {"$lt" (date "2021-01-01T00:00:00Z")}}]
                            }}))

(clojure.pprint/pprint (.explain (q :testdb.testcoll
                  { "$match" {
                              "samples.n" {"$gt" (date "2019-01-01T00:00:00Z")
                                           "$lt" (date "2021-01-01T00:00:00Z")}
                              }})))