(ns cmql-j.driver.cursor
  (:require clojure.pprint
            [cmql-j.driver.document :refer [json->clj]])
  (:import (java.util ArrayList)
           (org.bson Document)))

;;cursor
(defn jc-take-all [docs-iterable]
  (let [iterator (.iterator docs-iterable)
        docs (ArrayList.)]
    (loop []
      (if (.hasNext iterator)
        (do (.add docs (.next iterator))
            (recur))
        docs))))

(defn c-take-all [docs-iterable]
  (let [iterator (.iterator docs-iterable)]
    (loop [docs []]
      (if (.hasNext iterator)
        (recur (conj docs (.next iterator)))
        docs))))

(defn c-print-all [docs-iterable]
  (if (instance? Document docs-iterable)
    (let [doc docs-iterable]
      (if (.isJDocument ^Document doc)
        (println (.toJson doc))
        (clojure.pprint/pprint doc)))
    (let [iterator (.iterator docs-iterable)]
      (loop []
        (if (.hasNext iterator)
          (do (let [doc (.next iterator)]
                (if (.isJDocument ^Document doc)
                  (println (.toJson doc))
                  (clojure.pprint/pprint doc)))
              (recur)))))))

(defn c-first-doc [docs-iterable]
  (let [iterator (.iterator docs-iterable)]
    (if (.hasNext iterator)
      (.next iterator)
      nil)))