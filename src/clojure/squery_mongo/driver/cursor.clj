(ns squery-mongo.driver.cursor
  (:require clojure.pprint
            [squery-mongo.driver.document :refer [json->clj j-doc->clj]]
            [squery-mongo.driver.settings :refer [defaults]])
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
        (let [doc (.next iterator)
              doc (if (defaults :clj?) (j-doc->clj doc) doc)]
          (recur (conj docs doc)))
        docs))))

(defn c-print-all [docs-iterable]
  (if (instance? Document docs-iterable)
    (let [doc docs-iterable]
      (if (not (defaults :clj?))                                               ;(.isJDocument ^Document doc)   ;;TODO WHEN CODEC
        (println (.toJson doc))
        (clojure.pprint/pprint (j-doc->clj doc))))
    (let [iterator (.iterator docs-iterable)]
      (loop []
        (if (.hasNext iterator)
          (do (let [doc (.next iterator)]
                (if  (not (defaults :clj?))                                       ;(.isJDocument ^Document doc)  ;;TODO WHEN CODEC
                  (println (.toJson doc))
                  (clojure.pprint/pprint (j-doc->clj doc))))
              (recur)))))))

(defn c-first-doc [docs-iterable]
  (let [iterator (.iterator docs-iterable)]
    (if (.hasNext iterator)
      (let [doc (.next iterator)]
        (if (defaults :clj?) (j-doc->clj doc) doc))
      nil)))