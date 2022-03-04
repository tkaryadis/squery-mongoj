(ns cmql-j.driver.document
  (:require ;[cheshire.core :refer [generate-string parse-string]]
            [cmql-core.utils :refer [ordered-map]]
            [clojure.data.json :refer [write-str read-str]]
            [cmql-j.driver.utils :refer [ISODate]] )
  (:import [com.mongodb BasicDBList]
           [clojure.lang IPersistentMap Named Keyword Ratio]
           [java.util List Map Date Set]
           org.bson.types.ObjectId
           (org.bson.types Decimal128)
           (org.bson Document)))

;;TODO while encoding,its possible that i found not only keywords but ratio,sets(sets maybe auto-convert to arrays) etc
;;either i never use those types(convert manually before insert) or i check while encoding

;;-----------------------2 types of Documents(1 with Clojure map inside,1 with LinkedHashMap inside)--------------------

(defn ^Document clj-doc
  "Returns a Document that has as member the Clojure map"
  ([]
   (Document. true))
  ([clojure-map]
   (Document. true ^Map clojure-map))
  ([k1 v1 & kvs]
   (let [omap (apply ordered-map (concat [k1 v1] kvs))]
     (Document. true ^Map omap))))

(defn clj-docs [& clj-maps]
  (mapv clj-doc clj-maps))

(defn ^Document j-doc
  "Returns a document in the driver default form
   The map becomes a LinkedHashMap,member of Document class"
  ([]
   (Document.))
  ([java-map]
   (Document. ^Map java-map)))

(defn j-docs [& java-maps]
  (mapv #(Document. ^Map %) java-maps))
  
(defn omap->map [m]
  (cond 
    (map? m)
    (reduce (fn [v k]
              (assoc v k (omap->map (get m k))))
            {}
            (keys m))
            
    (vector? m)
    (mapv (fn [v] (omap->map v)) m)
    
    :else
    m))

;;-----------------------------------------Convert----------------------------------------------------------------------
;;1)clj->j-doc => clojure map is converted to a Document class with LinkedHashMap inside(the default Document of java)
;;2)j-doc->clj => Document is converted to clojure map with keyword keys
;;  j-doc->clj-str => >> with string keys

;;Clojure-map(or Document) -> Document java
(defprotocol CljToDoc
  (^org.bson.Document clj->j-doc [input] "Clojure map to org.bson.Document"))

(extend-protocol CljToDoc
  nil
  (clj->j-doc [input]
    nil)

  String
  (clj->j-doc [^String input]
    input)

  Boolean
  (clj->j-doc [^Boolean input] input)

  java.util.Date
  (clj->j-doc [^java.util.Date input] input)

  Ratio
  (clj->j-doc [^Ratio input] (double input))

  Keyword
  (clj->j-doc [^Keyword input] (.getName input))

  Named
  (clj->j-doc [^Named input] (.getName input))

  IPersistentMap
  (clj->j-doc [^IPersistentMap input]
    (let [o (j-doc)]
      (doseq [[k v] input]
        (.put o (clj->j-doc k) (clj->j-doc v)))
      o))

  List
  (clj->j-doc [^List input] (doall (map clj->j-doc input)))

  Set
  (clj->j-doc [^Set input] (doall (map clj->j-doc input)))

  Document
  (clj->j-doc [^Document input] input)

  com.mongodb.DBRef
  (clj->j-doc [^com.mongodb.DBRef dbref] dbref)

  Object
  (clj->j-doc [input] input))

(defprotocol DocToClj
  (j-doc->clj          [input]   "org.bson.Document to Clojure map with keyword keys")
  (j-doc->clj-str      [input]   "org.bson.Document to Clojure map with string keys"))

(extend-protocol DocToClj
  nil
  (j-doc->clj     [input] input)
  (j-doc->clj-str [input] input)

  Object
  (j-doc->clj     [input] input)
  (j-doc->clj-str [input] input)

  Decimal128
  (j-doc->clj     [^Decimal128 input] (.bigDecimalValue input))
  (j-doc->clj-str [^Decimal128 input] (.bigDecimalValue input))

  List
  (j-doc->clj     [^List input] (mapv j-doc->clj input))
  (j-doc->clj-str [^List input] (mapv j-doc->clj-str input))

  BasicDBList
  (j-doc->clj [^BasicDBList input] (mapv j-doc->clj input))
  (j-doc->clj-str [^BasicDBList input] (mapv j-doc->clj-str input))

  com.mongodb.DBRef
  (j-doc->clj [^com.mongodb.DBRef input] input)
  (j-doc->clj-str [^com.mongodb.DBRef input] input)

  Document
  (j-doc->clj [^Document input]
    (reduce (fn [m ^String k]
              (assoc m (keyword k) (j-doc->clj (.get input k))))
            {}
            (.keySet input)))
  (j-doc->clj-str [^Document input]
    (reduce (fn [m ^String k]
              (assoc m k (j-doc->clj-str (.get input k))))
            {}
            (.keySet input))))

(defprotocol ConvertToObjectId
  (^org.bson.types.ObjectId to-object-id [input]
    "Instantiates ObjectId from input unless the input itself is an ObjectId instance. In that case, returns input as is."))

(extend-protocol ConvertToObjectId
  String
  (to-object-id [^String input]
    (ObjectId. input))

  Date
  (to-object-id [^Date input]
    (ObjectId. input))

  ObjectId
  (to-object-id [^ObjectId input]
    input))


;;----------------------------------------------utils-------------------------------------------------------------------

(defn ^String clj->json
  "Clojure map to JSON string"
  [m & args]
  (apply write-str (cons m args)))

(defn ^Map json->clj
  "JSON string to Clojure-map"
  [m & args]
  (apply read-str (cons m args)))


;;------------------------------------------json-extended---------------------------------------------------------------

(declare  clj-ext->clj)

(defn read-vector [v]
  (loop [v v
         v1 []]
    (if (empty? v)
      v1
      (let [mb (first v)]
        (cond
          (map? mb)
          (let [mb (clj-ext->clj mb )]
            (recur (rest v) (conj v1 mb)))

          (vector? mb)
          (let [mb (read-vector mb )]
            (recur (rest v) (conj v1 mb)))

          :else
          (recur (rest v) (conj v1 mb)))))))

(defn add-type [m]
  (cond
    (contains? m "$oid")
    (ObjectId. ^String (get m "$oid"))

    (contains? m "$date")
    (ISODate ^String (get m "$date"))

    ;;TODO ADD ALL

    :else
    m
    ))

(defn read-map [m]
  (loop [ks (keys m)
         m1 (ordered-map)]
    (if (empty? ks)
      m1
      (let [k (first ks)
            vl (get m k)
            k (if (keyword? k) (name k) k)]
        (cond
          (map? vl)
          (let [vl (clj-ext->clj vl)]
            (recur (rest ks) (assoc m1 k vl)))

          (vector? vl)
          (let [vl (read-vector vl)]
            (recur (rest ks) (assoc m1 k vl)))

          :else
          (recur (rest ks) (assoc m1 k vl)))))))

(defn clj-ext->clj
  "Converts extented-json types, to binary data, argument is a vector or a map"
  [m]
  (if (vector? m)
    (read-vector m)
    (if (and (= (count m) 1) (clojure.string/starts-with? (name (first (keys m))) "$"))
      (let [m (add-type m)]
        (if-not (map? m)
          m
          (read-map m)))
      (read-map m))))

