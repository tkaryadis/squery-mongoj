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
  (let [print-option (get command-body "print")
        command (merge (ordered-map command-head) (dissoc command-body "print"))]
    (cond
      (= print-option "clj")
      (clojure.pprint/pprint command)

      (= print-option "js")
      (println (clj->json command))

      :else
      (clojure.pprint/pprint command))))