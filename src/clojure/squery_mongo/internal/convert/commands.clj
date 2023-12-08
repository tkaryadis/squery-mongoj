(ns squery-mongo.internal.convert.commands
  (:require [squery-mongo-core.internal.convert.commands :refer [split-db-namespace]]
            [squery-mongo.driver.settings :refer [defaults]])
  (:import (com.mongodb.client MongoClient MongoDatabase MongoCollection)
           (org.bson Document)
           (com.mongodb MongoClientSettings)))


(defn get-db-namespace
  "Commands are allowed to be called with object arguments like MongoCollection
   But to get the command we need string format"
  [command-info]
  (cond

    (keyword? command-info)
    command-info

    (instance? MongoCollection command-info)
    (let [mongoNamespace (.getNamespace ^MongoCollection command-info)
          coll-name (.getCollectionName mongoNamespace)
          db-name (.getDatabaseName mongoNamespace)]
      (keyword (str db-name "." coll-name)))

    (instance? MongoDatabase command-info)
    (keyword (str (.getName ^MongoDatabase command-info)))

    :else                                                   ;;coll-info
    (let [db-name (name (get command-info :db-name (.getName (defaults :db))))
          db-namespace (cond

                         (contains? command-info :coll)
                         (let [coll (get command-info :coll)
                               mongoNamespace (.getNamespace ^MongoCollection coll)
                               coll-name (.getCollectionName mongoNamespace)
                               db-name (.getDatabaseName mongoNamespace)]
                           (keyword (str db-name "." coll-name)))

                         (contains? command-info :coll-name)
                         (keyword (str db-name "." (name (get command-info :coll-name))))

                         :else
                         (keyword db-name))]
      db-namespace)))

(defn command-info-from-db-namespace [db-namespace]
  (let [[db-name coll-name] (split-db-namespace db-namespace)
        client-settings ^MongoClientSettings  (defaults :client-settings)
        client (defaults :client)
        session (defaults :session)
        registry (.getCodecRegistry client-settings)
        result-class Document                               ;;if i want different i should use the command-info-map
        db (if (some? db-name)                              ;;i always need a database,if not i use the default
             (.getDatabase client db-name)
             (defaults :db))
        db-name (.getName ^MongoDatabase db)
        coll (if (not= coll-name "") (.getCollection db coll-name result-class) nil) ;;coll is optional depends on command
        ]
    {:client client
     :session session
     :registry registry
     :result-class result-class
     :db db
     :coll coll
     :db-name db-name
     :coll-name coll-name
     :complete? true}))

(defn command-info-from-coll [^MongoCollection coll]
  (let [client (defaults :client)
        session (defaults :session)
        registry (.getCodecRegistry ^MongoCollection coll)
        result-class (.getDocumentClass coll)
        db (.withCodecRegistry ^MongoDatabase (.getDatabase client (.getDatabaseName (.getNamespace coll))) registry)
        db-name (.getName db)
        coll-name (.getCollectionName (.getNamespace coll))]
    {:client client
     :session session
     :registry registry
     :result-class result-class
     :db db
     :coll coll
     :db-name db-name
     :coll-name coll-name
     :complete? true}))

(defn command-info-from-database [^MongoDatabase db]
  (let [client (defaults :client)
        session (defaults :session)
        registry (.getCodecRegistry db)
        result-class Document
        db-name (.getName db)]
    {:client client
     :session session
     :registry registry
     :result-class result-class
     :db db
     :coll nil
     :db-name db-name
     :coll-name nil
     :complete? true}))

(defn command-info-from-command-info [command-info]
  (let [client (get command-info :client (defaults :client))
        session (get command-info :session (defaults :session))
        ;;registry from coll if i have,then from db if i have,and last from clientSetting(that he gave or the default)
        registry (cond
                   (contains? command-info :coll)
                   (.getCodecRegistry ^MongoCollection (get command-info :coll))

                   (contains? command-info :db)
                   (.getCodecRegistry ^MongoDatabase (get command-info :db))

                   :else
                   (.getCodecRegistry ^MongoClientSettings (get command-info :client-settings (defaults :client-settings))))
        result-class (get command-info :result-class Document)
        db-name (name (get command-info :db-name (.getName (defaults :db))))
        db (get command-info :db (.getDatabase ^MongoClient client db-name))
        coll-name (name (get command-info :coll-name))
        coll (cond

               (contains? command-info :coll)
               (get command-info :coll)

               (string? coll-name)
               (.getCollection ^MongoDatabase db coll-name)

               :else
               nil)]
    {:client client
     :session session
     :registry registry
     :result-class result-class
     :db db
     :coll coll
     :db-name db-name
     :coll-name coll-name
     :complete? true}))

(defn get-command-info
  "Arguments
    :db-name.coll-name
    coll(binary)
    command-info (override the defaults or add more than the defaults)
    {
      :client client  ;;override default in defaults
      :session        ;;overrride default in defaults (default = nil or user setted)
      :registry       ;;override client's registry
      :result-class   ;;override default (default = Document or user setted)

      ;;db or coll or db-namespace (1 from the 3)(all need db,some coll)
      :db-namespace OR :db OR :coll
    }
    returns a command-info-map in all cases (session/coll/coll-name might be nil)
    {
     :client client
     :session session
     :registry registry
     :result-class result-class
     :db db
     :coll coll
     :db-name db-name
     :coll-name coll-name
    }
  "
  [command-info]
  (cond

    (keyword? command-info)
    (command-info-from-db-namespace command-info)

    (instance? MongoCollection command-info)
    (command-info-from-coll command-info)

    (instance? MongoDatabase command-info)
    (command-info-from-database command-info)

    :else                                                 ;;coll-info-map
    (command-info-from-command-info command-info)))