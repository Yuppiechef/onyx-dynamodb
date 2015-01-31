(ns onyx.plugin.input-test
  (:require [midje.sweet :refer :all]
            [onyx.plugin.dynamodb]
            [taoensso.faraday :as far]
            [onyx.queue.hornetq-utils :as hq-utils]
            [onyx.api]))

;; Before running this test, be sure to setup a local dynamodb instance and have it running
;; http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html

;; Setup the environment
(def id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def scheduler :onyx.job-scheduler/round-robin)

(def env-config
  {:hornetq/mode :standalone
   :hornetq/server? true
   :hornetq.server/type :embedded
   :hornetq.embedded/config ["hornetq/non-clustered-1.xml"]
   :hornetq.standalone/host (:host (:non-clustered (:hornetq config)))
   :hornetq.standalone/port (:port (:non-clustered (:hornetq config)))
   :zookeeper/address (:address (:zookeeper config))
   :zookeeper/server? true
   :zookeeper.server/port (:spawn-port (:zookeeper config))
   :onyx/id id
   :onyx.peer/job-scheduler scheduler})

(def peer-config
  {:hornetq/mode :standalone
   :hornetq.standalone/host (:host (:non-clustered (:hornetq config)))
   :hornetq.standalone/port (:port (:non-clustered (:hornetq config)))
   :zookeeper/address (:address (:zookeeper config))
   :onyx/id id
   :onyx.peer/inbox-capacity (:inbox-capacity (:peer config))
   :onyx.peer/outbox-capacity (:outbox-capacity (:peer config))
   :onyx.peer/job-scheduler scheduler})

(def env (onyx.api/start-env env-config))

;; Setup the backing db (dynamodb)
(def client-opts
  {:access-key "TEST"
   :secret-key "TEST"
   :endpoint "http://localhost:8000"})

(far/create-table
 client-opts :people
 [:id :n]
 {:throughput {:read 1 :write 1}
  :block? true})

(def people
  [{:id 1 :name "Mike"}
   {:id 2 :name "Dorrene"}
   {:id 3 :name "Benti"}
   {:id 4 :name "Derek"}
   {:id 5 :name "Kristen"}])

(far/batch-write-item client-opts {:people {:put people}})

(def batch-size 1000)

(def hq-config {"host" (:host (:non-clustered (:hornetq config)))
                "port" (:port (:non-clustered (:hornetq config)))})

(def out-queue (str (java.util.UUID/randomUUID)))

(hq-utils/create-queue! hq-config out-queue)

(def workflow {:scan :persist})

(def catalog
  [{:onyx/name :segment
    :onyx/ident :dynamodb/segment
    :onyx/type :input
    :onyx/medium :dynamodb-scan
    :onyx/consumption :concurrent

    ;; See the following link for total-segments consideration. The onyx/batch-size will be used as :limit
    ;; http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#QueryAndScanParallelScan
    :dynamodb/total-segments 3
    :onyx/batch-size batch-size
    :onyx/doc "Creates ranges over an :eavt index to parellelize loading datoms downstream"}
   
   {:onyx/name :scan
    :onyx/ident :dynamodb/scan
    :onyx/type :function
    :onyx/fn :onyx.plugin.dynamodb/read-scan
    :onyx/consumption :concurrent
    :dynamodb/config client-opts
    :dynamodb/scan-options {:return :all-attributes}
    :dynamodb/table :people
    :onyx/batch-size batch-size
    :onyx/doc "Creates ranges over an :eavt index to parellelize loading datoms downstream"}

   {:onyx/name :persist
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name out-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size
    :onyx/doc "Output source for intermediate query results"}])

(def v-peers (onyx.api/start-peers! 1 peer-config))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :task-scheduler :onyx.task-scheduler/round-robin})

(def results (hq-utils/consume-queue! hq-config out-queue 1))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

(fact (into #{} (mapcat #(apply concat %) (map :names results)))
      => #{"Mike" "Benti" "Derek"})

