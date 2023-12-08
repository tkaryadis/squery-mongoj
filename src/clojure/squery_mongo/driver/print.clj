(ns squery-mongo.driver.print
  (:require [squery-mongo-core.utils :refer [string-map ordered-map]]
            [squery-mongo.driver.document :refer [clj->json]]))

(defn print-command? [args]
  (reduce (fn [[args print?] arg]
            (if print?
              [(conj args arg) print?]
              (if (and (map? arg)
                       (or (contains? arg :print)
                           (contains? arg "print")))
                [args true]
                [(conj args arg) print?])))
          [[] false]
          args))

(defn print-command [command-head command-body]
  (let [_ (prn "xxx" command-body)
        command (merge (ordered-map command-head) (dissoc command-body "print"))]
    (clojure.pprint/pprint command)))