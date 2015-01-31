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
    [:done] ;; <== What should this be?! ai.
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
       (map (fn [r] {:input task :message r}) result)})))

(defmethod p-ext/compress-batch [:input :dynamodb-scan]
  [{:keys [onyx.core/decompressed] :as pipeline}]
  {:onyx.core/compressed decompressed})

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
