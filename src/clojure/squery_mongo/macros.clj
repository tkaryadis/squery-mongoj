(ns squery-mongo.macros
  (:require squery-mongo-core.operators.operators
            squery-mongo-core.operators.stages
            [squery-mongo.internal.convert.arguments :refer [convert-arg]]))

;;-----------------------------------------DSL-macros--------------------------------------------------------------------

(defmacro squery
  "squery code should be under this enviroment,this enviroment is auto-included from query macros.
   But its not included in the operators
   (defn myf []
     (cqml (+ 1 2)))   ;;to generate {'$add' [1 2]}
   "
  [squery-code]
  `(let ~squery-mongo-core.operators.operators/operators-mappings
     ~squery-code))



(defmacro defmfn
  "Define like squery function,arguments :myvar. call without arguments"
  [f-name args body]
  `(def ~f-name (clojure.core/let ~squery-mongo-core.operators.operators/operators-mappings
                 (squery-mongo-core.operators.operators/fn ~args ~body))))

(defmacro defnmfn
  "Define like clojure function,normal clojure symbol arguments call with arguments"
  [f-name args body]
  `(defn ~f-name ~args (clojure.core/let ~squery-mongo-core.operators.operators/operators-mappings
                         (squery-mongo-core.operators.operators/fn ~args ~body))))

;;-----------------------------------------General method call----------------------------------------------------------

;;squery-mongo doesn't wrap the driver methods one by one (it has commands, that some wrap methds like q wraps .aggregate)
;;uses squery arguments and interop directly
;;here is an alternative by using a general m macro wrapper that converts squery arguments to java arguments
;;but its not used in general, but still its an option

(defmacro m
  "Takes a Java driver method name and the method arguments.
   It also accepts squery arguments
   1)squery-pipeline
     {:pipeline pipeline-in-java}
   2)vector become ArrayList
   3)squery options for example {:option1 value1} , are converted also
     this is for methods that take the options at call time
     for example insertMany has the InsertManyOptions,no need to give an instance
     Instance is created from method name,and options are converted and applied"
  [method-name & args]
  (loop [args args
         l (list (symbol (str "." (name method-name))))]
    (if (empty? args)
      l                                                     ;`(squery ~l)
      (recur (rest args) (concat l (list `(convert-arg ~(first args) ~(name method-name))))))))