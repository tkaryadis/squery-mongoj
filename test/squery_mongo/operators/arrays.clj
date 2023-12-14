(ns squery-mongo.operators.arrays
  (:refer-clojure :only [])
  (:use squery-mongo-core.operators.operators
        squery-mongo-core.operators.qoperators
        squery-mongo-core.operators.uoperators
        squery-mongo-core.operators.stages
        squery-mongo-core.operators.options
        squery-mongo.driver.cursor
        squery-mongo.driver.document
        squery-mongo.driver.settings
        squery-mongo.driver.transactions
        squery-mongo.driver.utils
        squery-mongo.arguments
        squery-mongo.commands
        squery-mongo.macros
        flatland.ordered.map
        clojure.pprint)
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (com.mongodb.client MongoClients MongoCollection MongoClient)
           (com.mongodb MongoClientSettings)
           (org.bson Document)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings))
                 :clj? true)

;;sort

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll [{:a [1 3 2] :b [{:c 1 :d 1} {:c 3} {:c 2}]}])

(c-print-all (q :testdb.testcoll
                {:a-asc (sort-array :a)
                 :a-des (sort-array :!a)
                 :project-array-and-sort (sort-array :b.c)
                 :doc-members (sort-array :b [:!c])}))