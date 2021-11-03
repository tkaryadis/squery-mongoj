(ns cmql-j.benchmark-gen
  (:refer-clojure :only [])
  (:use cmql-j.api
        cmql-j.client
        cmql-j.cursor
        cmql-j.document
        cmql-j.settings
        cmql.operators.operators
        cmql.operators.stages
        clojure.pprint)
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (com.mongodb ExplainVerbosity)
           (com.mongodb.client AggregateIterable)))


;;

(init-defaults {:client (connect) :decode "clj"})

;(drop-collection :testdb.testcoll)

#_(def mydocs [{:_id 1
              :samples [{:timestamp 5 :id 1}
                        {:timestamp 10 :id 2}
                        {:timestamp 15 :id 3}]}

             {:_id 2
              :samples [{:timestamp 20 :id 4}
                        {:timestamp 25 :id 5}
                        {:timestamp 30 :id 6}]}])

;;n docs,m members the array
(defn add-batch-docs [start-index1 ndocs start-index2 nmembers]
  (let [limit1 (+ start-index1 ndocs)]
    (loop [start-index1 start-index1
           start-index2 start-index2
           docs []]
      (if (= limit1 start-index1)
        (do (insert :testdb.testcoll docs)
            [start-index1 start-index2])
        (let [min-time start-index2
              [subdocs start-index2]
              (let [limit2 (+ start-index2 nmembers)]
                (loop [start-index2 start-index2
                       subdocs []]
                  (if (= start-index2 limit2)
                    [subdocs start-index2]
                    (recur (inc start-index2)
                           (conj subdocs {:timestamp start-index2
                                          :a         1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7 :h 8 :j 9 :k 10})))))
              doc {:_id     start-index1
                   :min-time min-time
                   :max-time start-index2
                   :samples subdocs}
              ;_ (prn start-index2)
              ]
          (recur (inc start-index1) start-index2 (conj docs doc)))))))

(defn add-docs [ndocs batch-size nmembers]
  (loop [start-index1 0
         start-index2 0]
    (if-not (>= start-index1 ndocs)
      (let [[start-index1 start-index2] (add-batch-docs start-index1 batch-size start-index2 nmembers)]
        (recur start-index1 start-index2)))))

;(time (add-docs 1000000 5000 20))

(create-index :testdb.testcoll (index [:min-time :max-time]))

(create-index :testdb.testcoll (index [:min-time]))

(create-index :testdb.testcoll (index [:max-time]))

#_(create-index :testdb.testcoll (index [:samples.timestamp]))

;(drop-index :testdb.testcoll [:samples.timestamp])

;;{ $and: [ { price: { $ne: 1.99 } }, { price: { $exists: true } } ] }
#_(time (prn (count (c-take-all (q :testdb.testcoll
                                 (match {"$and" [{"min-time" {"$lte" 40}} {"max-time" {"$gte" 20}}]})
                                 {:explain ExplainVerbosity/QUERY_PLANNER})))))

#_(clojure.pprint/pprint (.explain  (q :testdb.testcoll
                   (match {"$and" [{"min-time" {"$lte" 40}} {"max-time" {"$gte" 20}}]}))))

#_(time (c-take-all  (q :testdb.testcoll
                      (match (and- (<=- :min-time 40)
                                   (>=- :max-time 20))))))

(time (c-take-all (q :testdb.testcoll
                     (<=- :min-time 40)
                     (>=- :max-time 20)
                     (<= :min-time 40)
                     ;{:print true}
                     )))

#_(time (c-take-all  (q :testdb.testcoll
                      (and- (<=- :min-time 40)
                            (>=- :max-time 20)))))

#_(time (c-take-all  (q :testdb.testcoll
                      (match (and- (<=- :min-time 40)
                                   (>=- :max-time 20))))))

#_(time (c-take-all  (q :testdb.testcoll
                      (<= :min-time 40)
                      (>= :max-time 20))))




#_(time (prn (count (c-take-all (q :testdb.testcoll
                                 (<= :min-time 40)
                                 (>= :max-time 20)
                                 #_(match {"$and" [{"min-time" {"$lte" 40}} {"max-time" {"$gte" 20}}]}))))))

#_(time (prn (count (c-take-all (q :testdb.testcoll
                                 (match {"$and" [{"samples.timestamp" {"$lte" 40}} {"samples.timestamp" {"$gte" 20}}]}))))))



#_(time (prn (count (c-take-all (q :testdb.testcoll
                                 {:samples (if- (let [:first-member. (get-in :samples [0 "timestamp"])
                                                      :last-member. (get-in :samples [{:index (dec (count :samples))} "timestamp"])]
                                                  (and (<= :first-member. 40) (>= :last-member. 20)))
                                                (filter (fn [:sample.] (and (>= :sample.a. 1)
                                                                            (>= :sample.timestamp. 20)
                                                                            (<= :sample.timestamp. 40)))
                                                        :samples)
                                                [])}
                                 (not-empty? :samples))))))

#_(time (prn (count (c-take-all (q :testdb.testcoll
                                 {:samples (filter (fn [:sample.] (and (>= :sample.a. 1)
                                                                       (>= :sample.timestamp. 20)
                                                                       (<= :sample.timestamp. 40)))
                                                   :samples)}
                                 (not-empty? :samples))))))



;(drop-collection :testdb.testcoll)

#_(insert :testdb.testcoll mydocs)

;;{"samples":{"$elemMatch":{
;         "timestamp1": {
;                      "$gte": datetime.strptime("2010-01-01 00:05:00", "%Y-%m-%d %H:%M:%S"),
;                      "$lte": datetime.strptime("2020-12-31 00:05:00", "%Y-%m-%d %H:%M:%S")
;         },
;         "id13":{"$gt":5}
;    }}}

#_(c-print-all (q :testdb.testcoll
                (match {:samples {"$elemMatch" {
                                                "timestamp" {"$gte" 20}
                                                "id" {"$gt" 5}}}})
                ;(> :sample.timestamp 10)
                ;(unwind-move-to-root :samples)
                ;(> :timestamp 10)
                #_(group {:_id :_id}
                       {:samples (conj-each {:timestamp :timestamp
                                             :id :id})})))

#_(c-print-all (q :testdb.testcoll
                {:samples (if- (or (<= (get :samples 0) 20)
                                   (>= (get :samples {:index (dec (count :samples))}) 30))
                               (filter (fn [:sample.] (and (>= :sample.timestamp. 20)
                                                           (> :sample.id. 5)))
                                       :samples)
                               [])}
                (not-empty? :samples)))

#_(c-print-all (q :testdb.testcoll
                (unwind :samples)
                (> :samples.timestamp 10)
                #_(group {:_id :_id}
                       {:samples (conj-each {:timestamp :samples.timestamp
                                             :id :samples.id})})))

#_(c-print-all (q :testdb.testcoll
                (unwind :samples)
                (> :samples.timestamp 10)
                (group {:_id :_id}
                       {:samples (conj-each {:timestamp :samples.timestamp
                                             :id :samples.id})})))