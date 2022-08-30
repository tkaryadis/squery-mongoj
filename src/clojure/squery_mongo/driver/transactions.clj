(ns squery-mongo.driver.transactions
  (:import (com.mongodb.client TransactionBody ClientSession)))

;;;TransactionOptions txnOptions = TransactionOptions.builder()
;;        .readPreference(ReadPreference.primary())
;;        .readConcern(ReadConcern.LOCAL)
;;        .writeConcern(WriteConcern.MAJORITY)
;;        .build();
(defn transaction-body [trans-f]
  (reify
    TransactionBody
    (execute [this]
      (trans-f))))

(defn commit [^ClientSession clientSession transaction-f]
  (.withTransaction clientSession (transaction-body (partial transaction-f clientSession))))