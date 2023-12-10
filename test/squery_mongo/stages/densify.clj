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

;;altitube range in collection is  600-1700
;;here i partition based on altitube and i will add values in each partition
;;from 600-1700 because i use "full" (even if partition doesnt start with 600 or ends in 1700)
;;i will add 600,800,1000,1200,1400,1600 (only if missing)
;;  (even range was 1800, i wouldnt add the last, its exclusive)
;;if instead of "full" i used "partition" range will be the partition range
(c-print-all (q :testdb.testcoll
                (densify :altitude
                         [:variety]
                         (step 200 "full"))
                ;{:command true}
                ))

;;range here is 0,1000-200 =  0,800  (1000 is exclusive,if i used 1001 for example it would be 0,1000)
(c-print-all (q :testdb.testcoll
                (densify :altitude
                         [:variety]
                         (step 200 [0 1001]))
                ;{:command true}
                ))