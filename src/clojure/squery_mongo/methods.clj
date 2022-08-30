(ns squery-mongo.methods
  (:require [squery-mongo.macros :refer [m]]))

;;squery-mongo doesn't wrap the driver methods one by one (it has commands, that some wrap methds like q wraps .aggregate)
;;uses squery arguments and interop directly
;;here is an alternative by using a general m macro wrapper that converts squery arguments to java arguments
;;but its not used in general, but still its an option as alternative of squery arguments

(defmacro aggregate [& args]
  `(m :aggregate ~@args))

(defmacro count-documents [& args]
  `(m :countDocuments ~@args))