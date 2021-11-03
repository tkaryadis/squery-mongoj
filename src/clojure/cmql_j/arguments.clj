(ns cmql-j.arguments
  (:require cmql-core.operators.operators
            [cmql-core.internal.convert.stages :refer [cmql-vector->cmql-map]]
            [cmql-j.internal.convert.arguments :refer [jp-f]]
            [cmql-j.internal.convert.options :refer [convert-options]]
            [cmql-j.driver.document :refer [clj->j-doc]])
  (:import (org.bson Document BSON)
           (java.util Map ArrayList)))


;;For use with Java raw Interop,keep the driver java method,but write the arguments in cMQL
(defmacro cp
  "Convert a cMQL pipeline to a Java MQL pipeline"
  [& stages]
  {:__pipeline__ `(apply jp-f (let ~cmql-core.operators.operators/operators-mappings
                                ~(into [] stages)))})

(defmacro ^ArrayList p
  "Convert a cMQL pipeline to a Java MQL pipeline"
  [& stages]
  `(apply jp-f (let ~cmql-core.operators.operators/operators-mappings
                 ~(into [] stages))))

(defmacro cf [& filters]
  {:__filter__ `(apply jp-f (let ~cmql-core.operators.operators/operators-mappings
                              ~(into [] filters)))})

(defmacro ^BSON f [& filters]
  `(.get (first (apply jp-f (let ~cmql-core.operators.operators/operators-mappings
                              ~(into [] filters))))
         "$match"))

(defmacro co [& options]
  {:__options__ `(apply (partial clojure.core/merge {}) ~(into [] options))})

(defmacro o [options-obj & options]
  `(convert-options (apply (partial clojure.core/merge {}) ~(into [] options))
                    ~options-obj))

(defn ^Document d
  "Clojure map to Document.No convertion.Its O(1),clj-map is stored as member of the Document."
  [clj-map]
  (Document. true ^Map clj-map))

(defn ds [& clj-maps]
  (mapv d clj-maps))

(defn project-a [obj projection-vector-map]
  (cond

    (vector? projection-vector-map)
    (.projection obj (clj->j-doc (cmql-vector->cmql-map projection-vector-map 0)))

    (map? projection-vector-map)
    (.projection obj (clj->j-doc projection-vector-map))

    :else
    (.projection obj projection-vector-map)))

(defn sort-a [obj sort-vector-map]
  (cond

    (vector? sort-vector-map)
    (.sort obj (clj->j-doc (cmql-vector->cmql-map sort-vector-map -1)))

    (map? sort-vector-map)
    (.sort obj (clj->j-doc sort-vector-map))

    :else
    (.sort obj sort-vector-map)))