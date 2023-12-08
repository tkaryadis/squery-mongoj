(ns squery-mongo.documents
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

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll [{:a 1}])

;;no need for db to even exist
(c-print-all (q :adb
                (docs [{:a 1}])))

(c-print-all (q :testdb.testcoll
                (plookup [:a :a]                            ;;no other coll
                         [(docs [{:a 1 :b 2}])]
                         :joined)
                ;(command)
                ))

(c-print-all (q :testdb.testcoll
                (plookup nil                                ;;no other-coll
                         [:a. :a]
                         [(docs [{:a 1 :b 2}])
                          (= :a. :a)]
                         :joined)
                ;(command)
                ))
