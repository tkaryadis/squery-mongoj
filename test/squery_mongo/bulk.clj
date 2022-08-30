(ns squery-mongo.bulk
  (:refer-clojure :only [])
  (:use squery-mongo.api.macros
        squery-mongo.api.arguments
        squery-mongo.api.mongo-collection
        squery-mongo.api.driver-core
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
           (com.mongodb WriteConcern)
           (com.mongodb.client.model CountOptions Filters InsertOneModel)))

(comment
(init-defaults {:client (connect) :decode "clj"})

(def db (.getDatabase @default-client "testdb"))
(def coll ^MongoCollection (.getCollection db "testcoll"))

(.drop coll)

;;// 1. Ordered bulk operation - order is guaranteed
;collection.bulkWrite(
;  Arrays.asList(new InsertOneModel<>(new Document("_id", 4)),
;                new InsertOneModel<>(new Document("_id", 5)),
;                new InsertOneModel<>(new Document("_id", 6)),
;                new UpdateOneModel<>(new Document("_id", 1),
;                                     new Document("$set", new Document("x", 2))),
;                new DeleteOneModel<>(new Document("_id", 2)),
;                new ReplaceOneModel<>(new Document("_id", 3),
;                                      new Document("_id", 3).append("x", 4))));
;
;
; // 2. Unordered bulk operation - no guarantee of order of operation
;collection.bulkWrite(
;  Arrays.asList(new InsertOneModel<>(new Document("_id", 4)),
;                new InsertOneModel<>(new Document("_id", 5)),
;                new InsertOneModel<>(new Document("_id", 6)),
;                new UpdateOneModel<>(new Document("_id", 1),
;                                     new Document("$set", new Document("x", 2))),
;                new DeleteOneModel<>(new Document("_id", 2)),
;                new ReplaceOneModel<>(new Document("_id", 3),
;                                      new Document("_id", 3).append("x", 4))),
;  new BulkWriteOptions().ordered(false));


;;BulkWriteResult	bulkWrite​(ClientSession clientSession, List<? extends WriteModel<? extends TDocument>> requests)
;Executes a mix of inserts, updates, replaces, and deletes.
;BulkWriteResult	bulkWrite​(ClientSession clientSession, List<? extends WriteModel<? extends TDocument>> requests, BulkWriteOptions options)
;Executes a mix of inserts, updates, replaces, and deletes.
;BulkWriteResult	bulkWrite​(List<? extends WriteModel<? extends TDocument>> requests)
;Executes a mix of inserts, updates, replaces, and deletes.
;BulkWriteResult	bulkWrite​(List<? extends WriteModel<? extends TDocument>> requests, BulkWriteOptions options)
;Executes a mix of inserts, updates, replaces, and deletes.

;;DeleteManyModel<T>
;;  DeleteOneModel​(Bson filter)
;;  DeleteOneModel​(Bson filter, DeleteOptions options)
;;DeleteOneModel<T>
;;  DeleteManyModel(Bson filter)
;;  DeleteManyModel(Bson filter, DeleteOptions options)
;;InsertOneModel<T>
;;   1 document argument
;;ReplaceOneModel<T>
;;  ReplaceOneModel(Bson filter, T replacement)
;;  ReplaceOneModel(Bson filter, T replacement, ReplaceOptions options)
;;UpdateManyModel<T>
;;  UpdateManyModel​(Bson filter, List<? extends Bson> update)
;   UpdateManyModel​(Bson filter, List<? extends Bson> update, UpdateOptions options)
;   UpdateManyModel​(Bson filter, Bson update)
;   UpdateManyModel​(Bson filter, Bson update, UpdateOptions options)
;;UpdateOneModel<T>
;;  UpdateOneModel​(Bson filter, List<? extends Bson> update)
;   UpdateOneModel​(Bson filter, List<? extends Bson> update, UpdateOptions options)
;   UpdateOneModel​(Bson filter, Bson update)
;   UpdateOneModel​(Bson filter, Bson update, UpdateOptions options)




(defn bulk-write1 [coll]
  (.bulkWrite coll
              [(InsertOneModel. (d {"id" 4}))]))

(defn bulk-write [coll]
  (.bulkWrite coll
              [(insert-one-model {"id" 5})]))

(c-print-all (.aggregate coll (p)))

(bulk-write1 coll)
(bulk-write coll)

(c-print-all (.aggregate coll (p)))

)