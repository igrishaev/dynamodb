(ns dynamodb.params
  (:require
   [dynamodb.encode :refer [encode encode-attrs]]
   [dynamodb.sql :as sql]))


(defn- key-alias []
  (str "#" (gensym "attr")))


(defn- val-alias []
  (str ":" (gensym "value")))


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


(defmethod set-param :default
  [_params param value]
  (throw (ex-info "Unknown parameter"
                  {:type ::wrong-parameter
                   :param param
                   :value value})))


(defparam :attr-defs
  [params attr-defs]
  (assoc params :AttributeDefinitions
         (for [[attr-name attr-type] attr-defs]
           {:AttributeName attr-name
            :AttributeType attr-type})))


(defparam :attr-names
  [params attr-names]
  (reduce-kv
   (fn [acc k v]
     (assoc-in acc
               [:ExpressionAttributeNames k]
               v))
   params
   attr-names))


(defparam :attr-values
  [params attr-values]
  (reduce-kv
   (fn [acc k v]
     (assoc-in acc
               [:ExpressionAttributeValues k]
               (encode v)))
   params
   attr-values))


(defn +join [acc string]
  (if acc
    (str acc ", " string)
    string))


(defparam :attrs-get
  [params attrs-get]
  (reduce
   (fn [acc attr]
     (cond
       (keyword? attr)
       (let [ka (key-alias)]
         (-> acc
             (assoc-in [:ExpressionAttributeNames ka] attr)
             (update :ProjectionExpression +join ka)))
       (string? attr)
       (-> acc
           (update :ProjectionExpression +join attr))
       :else
       (throw (ex-info "Wrong attribute" {:attr attr}))))
   params
   attrs-get))


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
  [params add-form]

  (if-not add-form
    params

    (loop [[kv & kvs] add-form
           acc (update params :UpdateExpression str \space "ADD")]

      (if-not kv
        acc
        (let [[k v] kv
              more? (some? kvs)
              ka (key-alias)
              va (val-alias)]

          (recur kvs
                 (cond-> acc

                   (keyword? k)
                   (->
                    (assoc-in [:ExpressionAttributeNames ka] k)
                    (update :UpdateExpression str " " ka))

                   (string? k)
                   (update :UpdateExpression str " " k)

                   :then
                   (update :UpdateExpression str \space)

                   :then
                   (->
                    (assoc-in [:ExpressionAttributeValues va] (encode v))
                    (update :UpdateExpression str va))

                   more?
                   (update :UpdateExpression str ","))))))))


(defparam :delete
  [params delete-form]

  (if-not delete-form
    params
    (loop [[kv & kvs] delete-form
           acc (update params :UpdateExpression str \space "DELETE")]

      (if-not kv
        acc
        (let [[k v] kv
              more? (some? kvs)
              ka (key-alias)
              va (val-alias)]
          (recur kvs
                 (cond-> acc

                   (keyword? k)
                   (->
                    (assoc-in [:ExpressionAttributeNames ka] k)
                    (update :UpdateExpression str " " ka))

                   (string? k)
                   (update :UpdateExpression str " " k)

                   :then
                   (update :UpdateExpression str " ")

                   :then
                   (->
                    (assoc-in [:ExpressionAttributeValues va] (encode v))
                    (update :UpdateExpression str va))

                   more?
                   (update :UpdateExpression str ","))))))))


(defparam :set
  [params set-form]

  (if-not set-form
    params
    (loop [[kv & kvs] set-form
           acc (update params :UpdateExpression str \space "SET")]

      (if-not kv
        acc
        (let [[k v] kv
              more? (some? kvs)
              ka (key-alias)
              va (val-alias)]
          (recur kvs
                 (cond-> acc

                   (keyword? k)
                   (->
                    (assoc-in [:ExpressionAttributeNames ka] k)
                    (update :UpdateExpression str " " ka))

                   (string? k)
                   (update :UpdateExpression str " " k)

                   :then
                   (update :UpdateExpression str " = ")

                   (sql/sql? v)
                   (update :UpdateExpression str v)

                   (not (sql/sql? v))
                   (->
                    (assoc-in [:ExpressionAttributeValues va] (encode v))
                    (update :UpdateExpression str va))

                   more?
                   (update :UpdateExpression str ","))))))))


(defparam :remove
  [params remove-form]

  (if-not remove-form
    params

    (loop [[k & ks] remove-form
           acc (update params :UpdateExpression str \space "REMOVE")]

      (if-not k
        acc
        (let [more? (some? ks)
              ka (key-alias)]

          (recur ks
                 (cond-> acc

                   (keyword? k)
                   (->
                    (assoc-in [:ExpressionAttributeNames ka] k)
                    (update :UpdateExpression str " " ka))

                   (string? k)
                   (update :UpdateExpression str " " k)

                   more?
                   (update :UpdateExpression str ","))))))))


(defn pre-process [params]
  (reduce-kv
   (fn [acc k v]
     (set-param acc k v))
   {}
   params))
