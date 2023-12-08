(ns squery-mongo.densify
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

(insert :testdb.testcoll [{
                           "altitude" 600,
                           "variety" "Arabica Typica",
                           "score" 68.3
                           },
                          {
                           "altitude" 750,
                           "variety" "Arabica Typica",
                           "score" 69.5
                           },
                          {
                           "altitude" 950,
                           "variety" "Arabica Typica",
                           "score" 70.5
                           },
                          {
                           "altitude" 1250,
                           "variety" "Gesha",
                           "score" 88.15
                           },
                          {
                           "altitude" 1700,
                           "variety" "Gesha",
                           "score" 95.5,
                           "price" 1029
                           }
                          ])

(c-print-all (q :testdb.testcoll
                (densify :altitude
                         [:variety]
                         (step 200 "full"))
                ;{:command true}
                ))