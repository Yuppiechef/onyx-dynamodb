(ns onyx.plugin.dynamodb
  (:require
   [taoensso.faraday :as far]
   [onyx.peer.task-lifecycle-extensions :as l-ext]
   [onyx.peer.pipeline-extensions :as p-ext]
   [taoensso.timbre :refer [info]]))

(defmethod l-ext/inject-lifecycle-resources
  :dynamodb/scan
  [_ {:keys [onyx.core/task-map]  :as pipeline}]
  {:dynamodb/last-prim-kvs (atom false)})

(defmethod p-ext/read-batch [:input :dynamodb-scan]
  [{:keys [onyx.core/task-map onyx.core/task :dynamodb/last-prim-kvs] :as pipeline}]
  
  (cond
    (nil? @last-prim-kvs)
    (do
      (info "Stopping")
      {:onyx.core/batch [{:input task :message :done}]})
    :else
    (let [opts (:dynamodb/scan-options task-map)
          last @last-prim-kvs
          opts (if last (assoc opts :last-prim-kvs last) opts)
          result
          (far/scan
           (:dynamodb/config task-map)
           (:dynamodb/table task-map)
           (assoc opts :limit (:onyx/batch-size task-map)))
          last-kv (-> result meta :last-prim-kvs)]
      (info "Result: " result)
      (reset! last-prim-kvs last-kv)
      {:onyx.core/batch
       (map (fn [r] {:input task :message r}) (concat result [:done]))})))

(defmethod p-ext/decompress-batch [:input :dynamodb-scan]
  [{:keys [onyx.core/batch] :as event}]
  {:onyx.core/decompressed (filter identity (map :message batch))})

(defmethod p-ext/strip-sentinel [:input :dynamodb-scan]
  [{:keys [onyx.core/decompressed]}]
  {:onyx.core/tail-batch? (= (last decompressed) :done)
   :onyx.core/requeue? false
   :onyx.core/decompressed (remove (partial = :done) decompressed)})

(defmethod p-ext/apply-fn [:input :dynamodb-scan]
  [{:keys [onyx.core/decompressed]}]
  {:onyx.core/results decompressed})

(defmethod p-ext/apply-fn [:output :dynamodb]
  [_] {})

(defmethod p-ext/compress-batch [:output :dynamodb]
  [{:keys [onyx.core/decompressed] :as pipeline}]
  {:onyx.core/compressed decompressed})

(defmethod p-ext/write-batch [:output :dynamodb]
  [{:keys [onyx.core/compressed onyx.core/task-map] :as pipeline}]
  (far/batch-write-item
   (:dynamodb/config task-map)
   {(:dynamodb/table task-map) {:put compressed}})
  {:onyx.core/written? true})
