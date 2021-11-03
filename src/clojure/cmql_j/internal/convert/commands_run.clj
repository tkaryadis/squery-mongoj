(ns cmql-j.internal.convert.commands-run
  (:require [cmql-core.utils :refer [ordered-map]]
            [cmql-j.internal.convert.commands :refer [get-command-info]]
            [cmql-j.internal.convert.options :refer [add-options]]
            [cmql-j.driver.settings :refer [defaults pojo-registry clj-registry j-registry]]
            [cmql-j.driver.document :refer [clj-doc clj->j-doc]]
            [cmql-j.driver.print :refer [print-command]])
  (:import (com.mongodb MongoCommandException MongoClientSettings)
           (com.mongodb.client MongoCollection ClientSession MongoDatabase MongoClient)
           (java.util ArrayList Collection Map List Arrays)
           (org.bson Document)
           (com.mongodb.client.model InsertManyOptions InsertOneOptions)))

;;------------------------------------------run-command-----------------------------------------------------------------
;;----------------------------------------------------------------------------------------------------------------------

;;TODO kapies commands xriazonte ta registry/result-class
;;kapies akoma kai ean i coll exi registy/result-class, prepei na tis agnoiso

(def pojo-return-commands #{"find" "aggregate"})

;;{:result-class org.bson.Document, :db #object[com.mongodb.client.internal.MongoDatabaseImpl 0x28e8888d "com.mongodb.client.internal.MongoDatabaseImpl@28e8888d"], :db-name "admin", :registry #object[org.bson.internal.ProvidersCodecRegistry 0x6097fca9 "org.bson.internal.ProvidersCodecRegistry@f8cce712"], :client #object[com.mongodb.client.internal.MongoClientImpl 0x35eee641 "com.mongodb.client.internal.MongoClientImpl@35eee641"], :coll nil, :complete? true, :coll-name nil, :session nil}
(defn run-command [command-info command]
  (if (get-in command [:command-body "command"])
    (let [command-head (ordered-map (get command :command-head))
          command-body (dissoc (get command :command-body) "command")]
      (merge command-head command-body))
    (let [command-info (if (get command-info :complete?) command-info (get-command-info command-info))
          session (get command-info :session)
          command-name (first (vals (get command :command-head)))
          db (get command-info :db)
          db (if (contains? pojo-return-commands command-name)
               db
               (.withCodecRegistry ^MongoDatabase db
                                   (.getCodecRegistry ^MongoClientSettings
                                                      (get command-info :client-settings (defaults :client-settings)))))
          result-class (if (contains? pojo-return-commands command-name)
                         (get command-info :result-class)
                         Document)
          ;db-namespace (coll-or-coll-info-to-db-namespace coll)
          command-body (get command :command-body)
          command-body (if (contains? command-body "print")
                         (do (print-command (get command :command-head) command-body)
                             (dissoc command-body "print"))
                         command-body)
          command-body (dissoc command-body "client" "session")
          mql-map (merge (ordered-map (get command :command-head)) command-body)
          mql-doc (clj-doc mql-map)]
      (if (some? session)
        (.runCommand db ^ClientSession session ^Document mql-doc ^Class result-class) ;;(c-schema (.runCommand db ^ClientSession session ^Document mql-doc))
        (.runCommand db ^Document mql-doc ^Class result-class)))))


;;---------------------------------------Methods------------------------------------------------------------------------

;;if this run,i insert a Class (not Document or clojure map)
;;if user gives registry i use it,else i use the default pojo-registry

;;coll => exi dimiourgithi me tin return class
;;db-namespace => den exo ,tin perno apo to proto document,kai ftiaxno tin coll (pojo-registry default)
;;command-info => perno coll,exi idi tin class


;;if namespace => i add the clazz as decode and its ready
;;if commnad-info was map,it would have registry+return class
;;if it was coll ,again it will have
;;TODO test it
(defn run-insert-pojo [command-info command clazz]
  (let [command-info (get-command-info command-info)
        session (get command-info :session)
        coll (.withCodecRegistry ^MongoCollection (.getCollection ^MongoDatabase (.withCodecRegistry
                                                                                   ^MongoDatabase
                                                                                   (get command-info :db)
                                                                                   pojo-registry)
                                                                  ^String (get command-info :coll-name)
                                                                  clazz)
                                 pojo-registry)
        options (get command :command-body)
        documents (get options "documents")
        options (dissoc options "documents")
        result (if (vector? documents)
                 (if (some? session)
                   (.insertMany coll session documents (add-options (InsertManyOptions.) options))
                   (.insertMany coll documents (add-options (InsertManyOptions.) options)))
                 (if (some? session)
                   (.insertOne coll session documents (add-options (InsertOneOptions.) options))
                   (.insertOne coll documents (add-options (InsertOneOptions.) options))))]
    result))

;;optional settings
;;{:client client
;; :registry "custom_class_only"
;; :decode "js/clj/custom_class" }


;;i will put registry in coll only,and result-class as option  {:decode ...}
(defn run-aggregation [command-info command]
  (if (get-in command [:command-body "command"])
    (let [command-head (ordered-map (get command :command-head))
          command-body (dissoc (get command :command-body) "command")]
      (merge command-head command-body))
    (let [coll (get command-info :coll)
          session (get command-info :session)
          result-class (get command-info :result-class)
          command-body (get command :command-body)
          command-body (if (contains? command-body "print")
                         (do (print-command (get command :command-head) command-body)
                             (dissoc command-body "print"))
                         command-body)
          pipeline (get command-body "pipeline")
          pipeline (ArrayList. ^Collection (map clj-doc pipeline))
          aggregateIterableImpl (if (some? session)
                                  (.aggregate ^MongoCollection coll ^ClientSession session pipeline ^Class result-class)
                                  (.aggregate ^MongoCollection coll pipeline ^Class result-class))
          options (dissoc command-body "pipeline")
          _ (add-options aggregateIterableImpl options)
          ]
      aggregateIterableImpl)))


(defn run-find [command-info command]
  (let [command-body (get command :command-body)
        coll (get command-info :coll)
        session (get command-info :session)
        result-class (get command-info :result-class)
        findIterable (if (some? session)
                       (.find ^MongoCollection coll ^ClientSession session ^Class result-class) ;;(c-schema (.runCommand db ^ClientSession session ^Document mql-doc))
                       (.find ^MongoCollection coll ^Class result-class))
        options command-body
        _ (add-options findIterable options)
        ]
    findIterable))


;;--------------------------------------Method or Command(depends on arguments)-----------------------------------------

(defn run-insert-method-or-command [command-info command]
  (let [command-body (get command :command-body)
        documents (get command-body "documents")
        first-doc (if (instance? List documents)
                    (first documents)
                    documents)]
    (if (or (instance? Map first-doc) (instance? Document first-doc))
      (let [command-info-map (get-command-info command-info)]
        (run-command command-info-map command))
      (run-insert-pojo command-info command (class first-doc)))))