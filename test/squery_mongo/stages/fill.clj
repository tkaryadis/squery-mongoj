(ns squery-mongo.fill
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

(insert :testdb.testcoll [{:a 1 :b nil} {:a 2 :b 4}])

#_(c-print-all (q :testdb.testcoll
                (fill nil nil {:b 20 :c 10})))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll [{:a 1 :b 2} {:a 2 :b nil} {:a 3 :b 10}])

(c-print-all (q :testdb.testcoll
                (fill nil [:a] {:b "linear"})))


;(try (drop-collection :testdb.testcoll) (catch Exception e ""))

#_(insert :testdb.testcoll [
                          {
                           :date (ISODate "2021-03-08"),
                           :restaurant "Joe's Pizza",
                           :score 90
                           },
                          {
                           :date (ISODate "2021-03-08"),
                           :restaurant "Sally's Deli",
                           :score 75
                           },
                          {
                           :date (ISODate "2021-03-09"),
                           :restaurant "Joe's Pizza",
                           :score 92
                           },
                          {
                           :date (ISODate "2021-03-09"),
                           :restaurant "Sally's Deli"
                           },
                          {
                           :date (ISODate "2021-03-10"),
                           :restaurant "Joe's Pizza"
                           },
                          {
                           :date (ISODate "2021-03-10"),
                           :restaurant "Sally's Deli",
                           :score 68
                           },
                          {
                           :date (ISODate "2021-03-11"),
                           :restaurant "Joe's Pizza",
                           :score 93
                           },
                          {
                           :date (ISODate "2021-03-11"),
                           :restaurant "Sally's Deli"
                           }
                          ])

;;locf
#_(c-print-all (q :testdb.testcoll
                (fill nil [:date] {:score "locf"})
                ;{:command true}
                ))
