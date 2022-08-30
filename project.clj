(defproject org.squery/squery-mongo-j "0.2.0-SNAPSHOT"
  :description "Query MongoDB with up to 3x less code (clj/java-driver)"
  :url "https://github.com/tkaryadis/cmql-j"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [
                 ;;clj
                 [org.squery/squery-mongo-core "0.2.0-SNAPSHOT"]
                 [org.clojure/clojure "1.10.0"]
                 [org.slf4j/slf4j-api "1.7.30"]
                 [org.mongodb/mongodb-driver-sync "4.2.3" :exclusions [org.mongodb/bson]]
                 [org.clojure/data.json "2.4.0"]  
                 [cheshire "5.10.0"]              ;;json alternative
                 [org.flatland/ordered "1.5.9"]]
  
  :plugins [[lein-codox "0.10.7"]]

  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]

  ;;:repl-options {:init-ns cmql-j.cmql-repl}
  ;;:main cmql-j.core
  ;;:aot [cmql-j.cmql-repl]
  )
