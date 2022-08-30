(ns squery-mongo.test-methods
  (:refer-clojure :only [])
  (:use squery-mongo.macros
        squery-mongo.interop.arguments
        squery-mongo.interop.mongo-collection
        squery-mongo.client
        squery-mongo.cursor
        squery-mongo.document
        squery-mongo.settings
        squery-mongo-core.operators.operators
        squery-mongo-core.operators.stages
        clojure.pprint)
  (:refer-clojure)
  (:require cmql.operators.stages
            [clojure.core :as c])
  (:import (com.mongodb.client MongoClients MongoClient MongoDatabase MongoCollection)
           (com.mongodb.client.model CountOptions)))

(comment
(def client (MongoClients/create))
(def db ^MongoDatabase (.getDatabase ^MongoClient client "joy"))
;(def coll ^MongoCollection (.withCodecRegistry (.getCollection ^MongoDatabase db "users") clj-registry))

(def coll ^MongoCollection (.getCollection ^MongoDatabase db "users"))


;;3 ways to call aggregate

(c-print-all (m :aggregate coll (cp (= :userid 224283837859889152)
                                    [:userid])))

(c-print-all (aggregate coll (cp (= :userid 224283837859889152)
                                 [:userid])))

(c-print-all (.aggregate coll (p (= :userid 224283837859889152)
                                 [:userid])))

;;count-documents for filter and options
;;*non interop does so little only in options i have one less argument
;; with cost to not see the arguments,and to have to add each method implementation
;; if i dont find another reason i will keep the interop only

(prn (m :countDocuments
        coll
        (cf (>- :userid 224283837859889152))
        (co {:limit 10})))

(prn (count-documents
        coll
        (cf (>- :userid 224283837859889152))
        (co {:limit 10})))


(prn (.countDocuments
       coll
       (f (>- :userid 224283837859889152))
       (o (CountOptions.)
          {:limit 10})))

)