(ns squery-mongo.driver.utils
  (:use clojure.pprint)
  (:import (java.time Instant)
           (java.sql Date)))

;;For example   {mydate (date "2019-01-01T00:00:00Z")}
(defn ISODate 
  ([] (Date. (.getTime ^java.util.Date (java.util.Date.))))
  ([date-s]
  (Date/from (Instant/parse date-s))))
  
(defn days-to-ms [ndays]
  (* ndays 24 60 60000))

(defn explain-index [cursor]
  (pprint (let [explain-doc (.explain cursor)]
            explain-doc
            #_(if (get explain-doc :stages)
              {:executionTimeMillis (get-in explain-doc [:stages 0 :$cursor :executionStats ])
               :totalKeysExamined (get-in explain-doc [:stages 0 :$cursor :executionStats ])
               :totalDocsExamined (get-in explain-doc [:stages 0 :$cursor :executionStats ])
               :nReturned (get-in explain-doc [:stages 0 :$cursor :executionStats ])}
              {:executionTimeMillis (get-in explain-doc [:executionStats ])
               :totalKeysExamined (get-in explain-doc [:executionStats ])
               :totalDocsExamined (get-in explain-doc [:executionStats ])
               :nReturned (get-in explain-doc [:executionStats ])}))))
               
(defn string-map
  "Makes keyword keys to strings"
  [m]
  (if (map? m)
    (reduce (fn [m-k k]
              (assoc m-k (if (keyword? k)
                           (name k)
                           k)
                         (get m k)))
            {}
            (keys m))
    m))

(defn keyword-map
  "Makes string keys to keywords"
  [m]
  (if (map? m)
    (reduce (fn [m-k k]
              (assoc m-k (if (string? k)
                           (keyword k)
                           k)
                         (get m k)))
            {}
            (keys m))
    m))

