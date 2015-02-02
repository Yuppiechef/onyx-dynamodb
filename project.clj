(defproject yuppiechef/onyx-dynamodb "0.5.0"
  :description "Onyx plugin for DynamoDB"
  :url "https://github.com/Yuppiechef/onyx-dynamodb"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.mdrogalis/onyx "0.5.0"]
                 [com.taoensso/faraday "1.5.0" :exclusions [[org.clojure/clojure]]]]
  :profiles {:dev {:dependencies [[midje "1.6.2"]
                                  [org.hornetq/hornetq-core-client "2.4.0.Final"]]
                   :plugins [[lein-midje "3.1.3"]]}})
