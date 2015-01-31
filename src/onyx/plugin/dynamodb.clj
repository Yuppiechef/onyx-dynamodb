(ns onyx.plugin.dynamodb
  (:require
   [taoensso.faraday :as far]
   [onyx.peer.task-lifecycle-extensions :as l-ext]
   [onyx.peer.pipeline-extensions :as p-ext]
   [taoensso.timbre :refer [info]]))

(defmethod p-ext/apply-fn [:input :dynamodb-scan]
  [{:keys [onyx.core/task-map] :as pipeline}]
  (let [total (:dynamodb/total-segments task-map)]
    {:onyx.core/results
     (map (fn [] {:segment % :total-segments total}) (range total))}))

(defmethod l-ext/inject-lifecycle-resources
  :dynamodb/scan
  [_ {:keys [onyx.core/task-map]  :as pipeline}]
  {:onyx.core/params
   [{:last-prim-kvs (atom {})
     :config (:dynamodb/config task-map)
     :table (:dynamodb/table task-map)
     :opts (:dynamodb/scan-options task-map)}]})

(defmethod p-ext/read-batch [:input :dynamodb-scan]
  [{:keys [onyx.core/task-map :dynamodb/last-prim-kvs] :as pipeline} ]
  (let [opts (:dynamodb/scan-options task-map)
        last @last-prim-kvs
        opts (if last (assoc opts :last-prim-kvs last) opts)
        result
        (far/scan
         (:dynamodb/config task-map)
         (:dynamodb/table task-map)
         (assoc
          opts
          :limit (:onyx/batch-size task-map)
          :total-segments (:onyx/total-segments task-map)))]
    (reset! last-prim-kvs (-> result meta :last-prim-kvs))
    result))

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
