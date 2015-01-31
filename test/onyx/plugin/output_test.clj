(ns onyx.plugin.output-test
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

;; Setup our test input stream (we should probably be using async here..)
(def input-queue-name (str (java.util.UUID/randomUUID)))

(def hq-config {"host" (:host (:non-clustered (:hornetq config)))
                "port" (:port (:non-clustered (:hornetq config)))})

(def in-queue (str (java.util.UUID/randomUUID)))

(hq-utils/create-queue! hq-config in-queue)

;; Define our data..
(def people
  [{:id 1 :name "Mike"}
   {:id 2 :name "Dorrene"}
   {:id 3 :name "Benti"}
   {:id 4 :name "Kristen"}
   {:id 5 :name "Derek"}])

;; And push it into the queue..
(hq-utils/write-and-cap! hq-config in-queue people 1)


;; Now lets define the workflow and catalog..
;; Bit of a weird ordering here since you'd usually setup your system first, but it does illustrate
;; the flexibility of Onyx
(def workflow {:in {:identity :out}})

(def catalog
  [{:onyx/name :in
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name in-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size 2}

   {:onyx/name :identity
    :onyx/fn :clojure.core/identity
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size 2}
   
   {:onyx/name :out
    :onyx/ident :dynamodb/commit-tx
    :onyx/type :output
    :onyx/medium :dynamodb
    :onyx/consumption :concurrent
    :dynamodb/table :people
    :dynamodb/config client-opts
    :onyx/batch-size 2
    :onyx/doc "Transacts segments to dynamodb"}])

;; Spin up a single peer and throw the job at it.
(def v-peers (onyx.api/start-peers! 1 peer-config))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :task-scheduler :onyx.task-scheduler/round-robin})

;; Get the results
(def results (far/scan client-opts :people))

;; Clean up
(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

(far/delete-table client-opts :people)

;; Assert that what we expected to happen, did.
(fact (set (map :name results)) => (set (map :name people)))


