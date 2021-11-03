(ns cmql-j.driver.utils
  (:import (java.time Instant)
           (java.sql Date)))

;;For example   {mydate (date "2019-01-01T00:00:00Z")}
(defn ISODate 
  ([] (Date. (.getTime ^java.util.Date (java.util.Date.))))
  ([date-s]
  (Date/from (Instant/parse date-s))))
  
(defn days-to-ms [ndays]
  (* ndays 24 60 60000))
