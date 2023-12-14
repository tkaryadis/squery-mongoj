(ns squery-mongo.operators.accumulators-some-array-use
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

;;---------------------------------------arrays+groups-----------------------
;;count is not really the same operator, for 1 arg is $size, and with 0 is $count
;;the others are the same

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll [{:a [1 2 3]}])

(c-print-all (q :testdb.testcoll
                {:first (first :a)
                 :take (take 2 :a)
                 :last (last :a)
                 :take-last (take-last 2 :a)
                 :sum (sum :a)
                 :avg (avg :a)
                 :min (min :a)
                 :minn (take-min 2 :a)
                 :max (max :a)
                 :maxn (take-max 2 :a)

                 ;;same name but different operators
                 :count (count :a)
                 :conj (conj :a 10)
                 :conj-distinct (conj :a 2)
                 }))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll [{:a 1}  {:a 3} {:a 2}])

(c-print-all (q :testdb.testcoll
                (sort :a)
                (group {:_id nil}
                       {:first (first :a)}
                       {:take (take 2 :a)}
                       {:last (last :a)}
                       {:take-last (take-last 2 :a)}
                       {:sum (sum :a)}
                       {:avg (avg :a)}
                       {:min (min :a)}
                       {:minn (take-min 2 :a)}
                       {:max (max :a)}
                       {:maxn (take-max 2 :a)}

                       ;;same names but different operators
                       {:count (count)}
                       {:conj (conj :a)}
                       {:conj-distinct (conj-distinct :a)}
                       )))

;;----------------------------groups+objects----------------------------------

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll [{:a 1}  {:b 3}])

(c-print-all (q :testdb.testcoll
                {:merged (merge :ROOT. {:c 10})}))

(c-print-all (q :testdb.testcoll
                (group {:_id nil}
                       {:merged (merge :ROOT.)})))

;;----------------------------groups-only-------------------------------------

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll [{:a 1}  {:a 3} {:a 2} {:a 2} {:a 4}])

(c-print-all (q :testdb.testcoll
                (sort :!a)
                (group {:_id nil}
                       {:first (sort-first [:a] :a)}
                       {:firstn (sort-take 2 [:a] :a)}
                       {:last (sort-last [:a] :a)}
                       {:lastn (sort-take-last 2 [:a] :a)}
                       )))