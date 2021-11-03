(ns cmql-j.methods
  (:require [cmql-j.macros :refer [m]]))

;;cmql-j doesn't wrap the driver methods one by one (it has commands, that some wrap methds like q wraps .aggregate)
;;uses cmql arguments and interop directly
;;here is an alternative by using a general m macro wrapper that converts cmql arguments to java arguments
;;but its not used in general, but still its an option as alternative of cmql arguments

(defmacro aggregate [& args]
  `(m :aggregate ~@args))

(defmacro count-documents [& args]
  `(m :countDocuments ~@args))