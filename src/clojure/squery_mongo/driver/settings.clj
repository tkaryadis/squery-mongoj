(ns squery-mongo.driver.settings
  (:import (com.mongodb MongoClientSettings)
           (org.bson.codecs.configuration CodecRegistries CodecProvider CodecRegistry)
           (java.util ArrayList)
           (org.bson.codecs.pojo PojoCodecProvider)
           (com.mongodb.client MongoClient MongoDatabase)))

;;defaults,i can add anything i want,but squery-mongo will look those if i dont give a argument it needs
#_{:client client                              ;;default
   :session session                            ;;default
   :db db                                      ;;default
   :coll coll                                  ;;no default user must always give this if needed

   ;;?TODO
   :registry registry                          ;;no default value,default to Document if decode=js/cljs,defaults to pojo if decode a Class!=Document
   :result-class result-class                 ;;for pojo
   }
(def defaults-map (atom {:client-settings (-> (MongoClientSettings/builder) (.build))
                         :clj? true
                         :session nil}))

(defn defaults [k]
  (get @defaults-map k))

(defn update-defaults [& kvs]
  (loop [kvs (partition 2 kvs)]
    (if-not (empty? kvs)
      (let [kv (first kvs)]
        (recur (do (swap! defaults-map assoc (first kv) (second kv)) (rest kvs)))))))

;;clojure registry
#_(def clj-registry
  (let [codec-registry (CodecRegistries/fromRegistries
                         (into-array [(MongoClientSettings/getDefaultCodecRegistry)]))
        _ (.setjavaRegistry ^CodecRegistries codec-registry false)]
    codec-registry))

(def j-registry (let [codec-registry (CodecRegistries/fromRegistries
                                       (into-array [(MongoClientSettings/getDefaultCodecRegistry)]))]
                  codec-registry))

(def pojo-registry
  (let [pojo-codec-registry (CodecRegistries/fromProviders (into-array ^CodecProvider [(-> (PojoCodecProvider/builder)
                                                                                           (.automatic true)
                                                                                           .build)]))
        codec-registry (CodecRegistries/fromRegistries (into-array ^CodecRegistry [(MongoClientSettings/getDefaultCodecRegistry)
                                                                                   pojo-codec-registry]))]
    codec-registry))
