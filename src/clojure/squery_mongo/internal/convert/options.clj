(ns squery-mongo.internal.convert.options
  (:require [squery-mongo-core.utils :refer [string-map]]
            [squery-mongo.driver.document :refer [clj-doc clj->j-doc]])
  (:import (java.util.concurrent TimeUnit)
           (com.mongodb.client.model EstimatedDocumentCountOptions
                                     DropIndexOptions
                                     CountOptions
                                     FindOneAndReplaceOptions
                                     ReplaceOptions
                                     FindOneAndUpdateOptions
                                     UpdateOptions
                                     FindOneAndDeleteOptions
                                     DeleteOptions
                                     InsertManyOptions
                                     InsertOneOptions
                                     Collation)))


(def methods-options
  {
   "insertOne"              {:options-obj  (fn [] (InsertOneOptions.))
                             :options-keys #{"bypassDocumentValidation"}}

   "insertMany"             {:options-obj  (fn [] (InsertManyOptions.))
                             :options-keys #{"bypassDocumentValidation"
                                             "ordered"}}

   "deleteMany"             {:options-obj  (fn [] (DeleteOptions.))
                             :options-keys #{"collation"
                                             "hint"
                                             "hintString"}}

   "deleteOne"              (get methods-options "deleteMany")

   "findOneAndDelete"       {:options-obj  (fn [] (FindOneAndDeleteOptions.))
                             :options-keys #{"collation"
                                             "hint"
                                             "hintString"
                                             "maxTime"
                                             "projection"
                                             "sort"}}

   "updateOne"              {:options-obj  (fn [] (UpdateOptions.))
                             :options-keys #{"arrayFilters"
                                             "bypassDocumentValidation"
                                             "collation"
                                             "hint"
                                             "hintString"
                                             "upsert"}}

   "findOneAndUpdate"       {:options-obj  (fn [] (FindOneAndUpdateOptions.))
                             :options-keys #{"arrayFilters"
                                             "bypassDocumentValidation"
                                             "collation"
                                             "hint"
                                             "hintString"
                                             "maxTime"
                                             "projection"
                                             "returnDocument"
                                             "sort"}}

   "replaceOne"             {:options-obj  (fn [] (ReplaceOptions.))
                             :options-keys #{"bypassDocumentValidation"
                                             "collation"
                                             "hint"
                                             "hintString"
                                             "upsert"}}

   "findOneAndReplace"      {:options-obj  (fn [] (FindOneAndReplaceOptions.))
                             :options-keys #{"bypassDocumentValidation"
                                             "collation"
                                             "hint"
                                             "hintString"
                                             "maxTime"
                                             "projection"
                                             "returnDocument"
                                             "sort"
                                             "upsert"}}

   "countDocuments"         {:options-obj  (fn [] (CountOptions.))
                             :options-keys #{"collation"
                                             "hint"
                                             "hintString"
                                             "limit"
                                             "maxTime"
                                             "skip"}}

   "dropIndex"              {:options-obj  (fn [] (DropIndexOptions.))
                             :options-keys #{"maxTime"}}

   "dropIndexes"            (get methods-options "dropIndex")

   "estimatedDocumentCount" {:options-obj  (fn [] (EstimatedDocumentCountOptions.))
                             :options-keys #{"maxTime"}}

   })

;;TODO
;;collation

;;if i find duplicates that require different functions,i add both options before calling add-options
;;if something can go in the method call or in option-map,it goes in option map (example filter in find)

;;when option is Bson , i need only clojure-map => clj-doc (test on all)
(def options-map
  {
   ;;Same name,same value (no convert needed)
   "allowDiskUse"             (fn [obj option] (.allowDiskUse obj option))
   "bypassDocumentValidation" (fn [obj option] (.bypassDocumentValidation obj option))
   "comment"                  (fn [obj option] (.comment obj option))
   "filter"                   (fn [obj option] (.filter obj (clj-doc option)))
   "sort"                     (fn [obj option] (.sort obj (clj-doc option)))
   "projection"               (fn [obj option] (.projection obj (clj-doc option)))
   "max"                      (fn [obj option] (.max obj (clj-doc option)))
   "min"                      (fn [obj option] (.min obj (clj-doc option)))
   "limit"                    (fn [obj option] (.limit obj option))
   "skip"                     (fn [obj option] (.skip obj option))
   "noCursorTimeout"          (fn [obj option] (.noCursorTimeout obj option))
   "oplogReplay"              (fn [obj option] (.oplogReplay obj option))
   "returnKey"                (fn [obj option] (.returnKey obj option))
   "returnDocument"           (fn [obj option] (.returnDocument obj option))
   "showRecordId"             (fn [obj option] (.showRecordId obj option))
   "upsert"                   (fn [obj option] (.upsert obj option))
   "isUpsert"                 (fn [obj option] (.upsert obj option))
   "arrayFilters"             (fn [obj option] (.arrayFilters obj option))
   "ordered"                  (fn [obj option] (.ordered obj option))


   ;;Same name,value convert
   "hint"                     (fn [obj option] (if (string? option)
                                                 (.hintString obj option)
                                                 (.hint obj (clj-doc option))))

   "collation"                (fn [obj option] (if (instance? Collation option)               ;TODO
                                                 nil
                                                 ;(.collation obj option)
                                                 ;(.collation obj (collation-map->collation-obj option))
                                                 ))

   ;;Different name,maybe value (but convertable)

   ;;Java only
   "hintString"               (fn [obj option] (.hintString obj option))
   "cursorType"               (fn [obj option] (.cursorType obj option))
   "maxAwaitTime"             (fn [obj option] (if (vector? option)
                                                 (.maxAwaitTime obj (first option) (second option))
                                                 (option obj)))
   "maxTime"                  (fn [obj option] (if (vector? option)
                                                 (.maxTime obj (first option) (second option))
                                                 (option obj)))
   "partial"                  (fn [obj option] (.partial obj option))

   ;;Command only
   "allowPartialResults"      (fn [obj option] (.partial obj option))
   "singleBatch"              (fn [obj option])          ;;TODO part of CursorType
   "tailable"                 (fn [obj option])          ;;TODO part of CursorType
   "cursor"                   (fn [obj option] (let [batchSize (get option "batchSize")]
                                                 (if batchSize (.batchSize obj batchSize))))
   "maxTimeMS"                (fn [obj option] (if (> option 0) (.maxTime obj option TimeUnit/MILLISECONDS)))

   ;;Not java option(not convertable)
   "readConcern"              (fn [obj option])          ;;TODO part of method call
   "awaitData"                (fn [obj option])
   })

(def options-map-keys-set (into #{} (keys options-map)))

(defn add-options [obj options]
  (loop [options-keys (keys options)
         options options]
    (if (empty? options)
      obj
      (let [option-key (first options-keys)
            optionfn (get options-map option-key)]
        (if (some? optionfn)
          (do (optionfn obj (get options option-key))
              (recur (rest options-keys) (dissoc options option-key)))
          (throw (Exception. (str "Unknown option = " option-key ))))))))

(defn option-arg? [arg method-name]
  (and (map? arg)
       (let [key-set (into #{} (map name (keys arg)))]
         (clojure.set/subset? key-set (get-in methods-options [method-name :options-keys] #{})))))

;;options should be 1 map {:option1 value1 :option2 value2 ...}
(defn convert-options [options method-name-or-options-obj]
  (let [options-obj (if (string? method-name-or-options-obj)
                      (let [method-options (get methods-options method-name-or-options-obj)
                            options-obj ((get method-options :options-obj))]
                        options-obj)
                      method-name-or-options-obj)
        options (string-map options)
        ;_ (prn options method-name (type options-obj))
        _ (add-options options-obj options)]
    options-obj))