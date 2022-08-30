(ns squery-mongo.arguments
  (:require squery-mongo-core.operators.operators
            [squery-mongo-core.internal.convert.stages :refer [squery-vector->squery-map]]
            [squery-mongo.internal.convert.arguments :refer [jp-f u-f]]
            [squery-mongo.internal.convert.options :refer [convert-options]]
            [squery-mongo.driver.document :refer [clj->j-doc]])
  (:import (org.bson Document BSON)
           (java.util Map ArrayList)))


;;For use with Java raw Interop,keep the driver java method,but write the arguments in squery
(defmacro cp
  "Convert a squery pipeline to a Java MQL pipeline"
  [& stages]
  {:__pipeline__ `(apply jp-f (let ~squery-mongo-core.operators.operators/operators-mappings
                                ~(into [] stages)))})

(defmacro  p
  "Convert a squery pipeline to a Java MQL pipeline(Arraylist)"
  [& stages]
  `(apply jp-f (let ~squery-mongo-core.operators.operators/operators-mappings
                 ~(into [] stages))))

(defmacro u
  "Converts a squery update(not pipeline update) to a Java MQL update"
  [& update-operators]
  `(apply u-f (let ~squery-mongo-core.operators.operators/operators-mappings
                ~(into [] update-operators))))

(defmacro f [& filters]
  `(.get (first (apply jp-f (let ~squery-mongo-core.operators.operators/operators-mappings
                              ~(into [] filters))))
         "$match"))

(defmacro mu [& update-operators]
  {"$__us__" (into [] update-operators)})

(defmacro mf [& query-operators]
  {"$__qs__" query-operators})

(defmacro o [options-obj & options]
  `(convert-options (apply (partial clojure.core/merge {}) ~(into [] options))
                    ~options-obj))

(defmacro cf
  [& filters]
  {:__filter__ `(apply jp-f (let ~squery-mongo-core.operators.operators/operators-mappings
                              ~(into [] filters)))})

(defmacro co [& options]
  {:__options__ `(apply (partial clojure.core/merge {}) ~(into [] options))})


(defn ^Document d
  "Clojure map to Document.No convertion.Its O(1),clj-map is stored as member of the Document."
  [clj-map]
  (Document. true ^Map clj-map))

(defn ds [& clj-maps]
  (mapv d clj-maps))

(defn project-a [obj projection-vector-map]
  (cond

    (vector? projection-vector-map)
    (.projection obj (clj->j-doc (squery-vector->squery-map projection-vector-map 0)))

    (map? projection-vector-map)
    (.projection obj (clj->j-doc projection-vector-map))

    :else
    (.projection obj projection-vector-map)))

(defn sort-a [obj sort-vector-map]
  (cond

    (vector? sort-vector-map)
    (.sort obj (clj->j-doc (squery-vector->squery-map sort-vector-map -1)))

    (map? sort-vector-map)
    (.sort obj (clj->j-doc sort-vector-map))

    :else
    (.sort obj sort-vector-map)))