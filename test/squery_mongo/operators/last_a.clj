(ns squery-mongo.operators.last-a
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

(insert :testdb.testcoll
        [
         {:playerId "PlayerA" :gameId "G1" :score 31}
         {:playerId "PlayerB" :gameId "G1" :score 33}
         {:playerId "PlayerC" :gameId "G1" :score 99}
         {:playerId "PlayerD" :gameId "G1" :score 1}
         {:playerId "PlayerA" :gameId "G2" :score 10}
         {:playerId "PlayerB" :gameId "G2" :score 14}
         {:playerId "PlayerC" :gameId "G2" :score 66}
         {:playerId "PlayerD" :gameId "G2" :score 80}
         ])

(c-print-all (q :testdb.testcoll
                (= :gameId "G1")
                (group {:_id :gameId}
                       {:playerId (last-a [:!score]
                                          [:playerId :score])})
                ;(command)
                ))

(c-print-all (q :testdb.testcoll
                (= :gameId "G1")
                (group {:_id :gameId}
                       {:playerId (lastn-a 2
                                           [:!score]
                                           [:playerId :score])})
                ;(command)
                ))
