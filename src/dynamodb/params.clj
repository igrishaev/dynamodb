(ns dynamodb.params
  (:require
   [clojure.string :as str]
   [dynamodb.util :as util]
   [dynamodb.encode :refer [encode-attrs]]
   [dynamodb.transform :as transform]))


(defn- -enc-attr-vals [attr-vals]
  (-> attr-vals
      (encode-attrs)
      (util/update-keys str)))


(defn- -enc-attr-to-get [attr-to-get]
  (->> attr-to-get
       (map transform/keyword->name-placeholder)
       (str/join ", ")))


(defn- -remap-key-schema [key-schema]
  (for [[attr-name key-type] key-schema]
    {:AttributeName attr-name
     :KeyType key-type}))


(defn- -remap-provisioned-throughput
  [provisioned-throughput]
  (let [[read-units write-units]
        provisioned-throughput]
    {:ReadCapacityUnits read-units
     :WriteCapacityUnits write-units}))


(defn- -remap-projection
  [{:keys [type non-key-attrs]}]
  {:NonKeyAttributes non-key-attrs
   :ProjectionType type})


(defn- -remap-indexes
  [global-indexes]
  (for [[idx {:keys [key-schema
                     projection
                     provisioned-throughput]}]
        global-indexes]
    (cond-> {:IndexName idx
             :KeySchema (-remap-key-schema key-schema)
             :Projection (-remap-projection projection)}
      provisioned-throughput
      (assoc :ProvisionedThroughput
             (-remap-provisioned-throughput
              provisioned-throughput)))))


(defn- -update-expression
  [params expr]
  (if expr
    (update params :UpdateExpression
            str
            \space
            expr)
    params))


;;
;; Params
;;

(defmulti set-param
  (fn [_params param _value]
    param))


(defmacro defparam
  {:style/indent 1}
  [dispatch-val [params value] & body]
  `(defmethod set-param ~dispatch-val
     [~params _# ~value]
     ~@body))


(defparam :attr-defs
  [params attr-defs]
  (assoc params :AttributeDefinitions
         (for [[attr-name attr-type] attr-defs]
           {:AttributeName attr-name
            :AttributeType attr-type})))


(defparam :attr-keys
  [params attr-keys]
  (assoc params :ExpressionAttributeNames
         (transform/encode-attr-names attr-keys)))


(defparam :attr-vals
  [params attr-vals]
  (assoc params :ExpressionAttributeValues
         (-enc-attr-vals attr-vals)))


(defparam :attrs-get
  [params attrs-get]
  (assoc params :ProjectionExpression
         (-enc-attr-to-get attrs-get)))


(defparam :backup
  [params backup]
  (assoc params :BackupName backup))


(defparam :backup-arn
  [params backup-arn]
  (assoc params :BackupArn backup-arn))


(defparam :billing-mode
  [params billing-mode]
  (assoc params :BillingMode billing-mode))


(defparam :consistent-read?
  [params consistent-read?]
  (assoc params :ConsistentRead consistent-read?))


(defparam :global-indexes
  [params global-indexes]
  (assoc params :GlobalSecondaryIndexes
         (-remap-indexes global-indexes)))


(defparam :index
  [params index]
  (assoc params :IndexName index))


(defparam :item
  [params item]
  (assoc params :Item (encode-attrs item)))


(defparam :key
  [params key]
  (assoc params :Key (encode-attrs key)))


(defparam :key-schema
  [params key-schema]
  (assoc params :KeySchema
         (-remap-key-schema key-schema)))


(defparam :keys
  [params keys]
  (assoc params :Keys
         (mapv encode-attrs keys)))


(defparam :limit
  [params limit]
  (assoc params :Limit limit))


(defparam :local-indexes
  [params local-indexes]
  (assoc params :LocalSecondaryIndexes
         (-remap-indexes local-indexes)))


(defparam :provisioned-throughput
  [params provisioned-throughput]
  (assoc params :ProvisionedThroughput
         (-remap-provisioned-throughput
          provisioned-throughput)))


(defparam :request-items
  [params request-items]
  (assoc params :RequestItems
         (reduce-kv
          (fn [acc table params-nested]
            (assoc acc table
                   (reduce-kv
                    (fn [acc k v]
                      (set-param acc k v))
                    {}
                    params-nested)))
          {}
          request-items)))


(defparam :resource-arn
  [params resource-arn]
  (assoc params :ResourceArn resource-arn))


(defparam :return-consumed-capacity
  [params return-consumed-capacity]
  (assoc params :ReturnConsumedCapacity
         return-consumed-capacity))


(defparam :return-item-collection-metrics
  [params return-item-collection-metrics]
  (assoc params :ReturnItemCollectionMetrics
         return-item-collection-metrics))


(defparam :return-values
  [params return-values]
  (assoc params :ReturnValues return-values))


(defparam :scan-forward?
  [params scan-forward?]
  (assoc params :ScanIndexForward scan-forward?))


(defparam :segment
  [params segment]
  (assoc params :Segment segment))


(defparam :select
  [params select]
  (assoc params :Select select))


(defparam :sql-condition
  [params sql-condition]
  (assoc params :ConditionExpression sql-condition))


(defparam :sql-filter
  [params sql-filter]
  (assoc params :FilterExpression sql-filter))


(defparam :sql-key
  [params sql-key]
  (assoc params :KeyConditionExpression sql-key))


(defparam :sse-spec
  [params {:keys [enabled? kms-key-id type]}]
  (assoc params :SSESpecification
         (cond-> {}

           (some? enabled?)
           (assoc :Enabled enabled?)

           kms-key-id
           (assoc :KMSMasterKeyId kms-key-id)

           type
           (assoc :SSEType type))))


(defparam :start-key
  [params start-key]
  (assoc params :ExclusiveStartKey
         (encode-attrs start-key)))


(defparam :start-table
  [params start-table]
  (assoc params :ExclusiveStartTableName start-table))


(defparam :stream-spec
  [params {:keys [enabled? view-type]}]
  (assoc params :StreamSpecification
         {:StreamEnabled enabled?
          :StreamViewType view-type}))


(defparam :table
  [params table]
  (assoc params :TableName table))


(defparam :table-class
  [params table-class]
  (assoc params :TableClass table-class))


(defparam :tags
  [params tags]
  (assoc params :Tags
         (for [[k v] tags]
           {:Key k
            :Value v})))


(defparam :total-segments
  [params total-segments]
  (assoc params :TotalSegments total-segments))


(defparam :add
  [params add]
  (-update-expression params (transform/add-expr add)))


(defparam :delete
  [params delete]
  (-update-expression params (transform/delete-expr delete)))


(defparam :set
  [params set]
  (-update-expression params (transform/set-expr set)))


(defparam :remove
  [params remove]
  (-update-expression params (transform/remove-expr remove)))


(defn pre-process [params]
  (reduce-kv
   (fn [acc k v]
     (set-param acc k v))
   {}
   params))
