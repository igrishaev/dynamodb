(ns dynamodb.api
  "
  BatchExecuteStatement
+ BatchGetItem
. BatchWriteItem
+ CreateBackup
  CreateGlobalTable
+ CreateTable
. DeleteBackup
+ DeleteItem
+ DeleteTable
+ DescribeBackup
  DescribeContinuousBackups
  DescribeContributorInsights
  DescribeEndpoints
  DescribeExport
  DescribeGlobalTable
  DescribeGlobalTableSettings
  DescribeImport
  DescribeKinesisStreamingDestination
. DescribeLimits
+ DescribeTable
  DescribeTableReplicaAutoScaling
  DescribeTimeToLive
  DisableKinesisStreamingDestination
  EnableKinesisStreamingDestination
  ExecuteStatement
  ExecuteTransaction
  ExportTableToPointInTime
+ GetItem
  ImportTable
. ListBackups
  ListContributorInsights
. ListExports
  ListGlobalTables
. ListImports
+ ListTables
. ListTagsOfResource
+ PutItem
+ Query
  RestoreTableFromBackup
  RestoreTableToPointInTime
+ Scan
+ TagResource
  TransactGetItems
  TransactWriteItems
. UntagResource
  UpdateContinuousBackups
  UpdateContributorInsights
  UpdateGlobalTable
  UpdateGlobalTableSettings
+ UpdateItem
  UpdateTable
  UpdateTableReplicaAutoScaling
  UpdateTimeToLive
  "
  (:import
   java.net.URI
   java.util.Map
   java.util.List)
  (:require
   [clojure.string :as str]
   [dynamodb.mask :as mask]
   [dynamodb.util :as util]
   [dynamodb.transform :as transform]
   [dynamodb.constant :as const]
   [dynamodb.client :as client]
   [dynamodb.decode :refer [decode-attrs]]
   [dynamodb.encode :refer [encode-attrs]]))


(defn- -enc-attr-vals [attr-vals]
  (-> attr-vals
      (encode-attrs)
      (util/update-keys str)))


(defn- -enc-attr-to-get [attr-to-get]
  (->> attr-to-get
       (map transform/keyword->name-placeholder)
       (str/join ", ")))


(defn- -enc-attr-keys [attr-keys]
  (transform/encode-attr-names attr-keys))


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


(defn- -remap-tags [tags]
  (for [[k v] tags]
    {:Key k
     :Value v}))


(defn- -remap-update-expression
  [add set remove delete]
  (transform/update-expression
   {:set set
    :add add
    :remove remove
    :delete delete}))


(defmulti set-param
  (fn [params param value]
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
         (-enc-attr-keys attr-keys)))


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
  (assoc params :Tags (-remap-tags tags)))


(defparam :total-segments
  [params total-segments]
  (assoc params :TotalSegments total-segments))


(defn- -update-expression
  [params expr]
  (if expr
    (update params :UpdateExpression
            str
            \space
            expr)
    params))


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
   params)

  #_
  (let [{:keys [
                add



                ]}
        params]

    (cond-> {}

      (or set add remove delete)
      (assoc :UpdateExpression
             (-remap-update-expression add set remove delete))





      )))


(defn- -decode-items [items]
  (mapv decode-attrs items))


(defn post-process [response]
  (when-not (= response {})
    (cond-> response

      (:Item response)
      (update :Item decode-attrs)

      (:Attributes response)
      (update :Attributes decode-attrs)

      (:Items response)
      (update :Items -decode-items)

      (:Responses response)
      (update :Responses util/update-vals -decode-items)

      (:UnprocessedKeys response)
      (update :UnprocessedKeys
              util/update-vals
              (fn [entry]
                (update entry :Keys -decode-items)))

      (:LastEvaluatedKey response)
      (update :LastEvaluatedKey decode-attrs))))


(defn make-client

  ([access-key secret-key endpoint region]
   (make-client access-key secret-key endpoint region nil))

  ([access-key secret-key endpoint region
    {:keys [throw?
            version]
     :or {throw? false
          version const/version-20120810}}]

   (let [uri
         (new URI endpoint)

         host
         (.getHost uri)

         path
         (.getPath uri)]

     {:access-key (mask/mask access-key)
      :secret-key (mask/mask secret-key)
      :endpoint endpoint
      :content-type "application/x-amz-json-1.0"
      :host host
      :path path
      :service "dynamodb"
      :version version
      :region region
      :throw? throw?})))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html
;; https://cloud.yandex.com/en-ru/docs/ydb/docapi/api-ref/actions/createTable
(defn create-table

  {:arglists
   '([client]
     [client
      {:keys [^Map    tags
              ^String table-class
              ^String billing-mode
              ^List   provisioned-throughput
              ^Map    global-indexes
              ^Map    local-indexes
              ^Map    stream-spec
              ^Map    sse-spec]}])}

  ([client table attr-defs key-schema]
   (create-table client table attr-defs key-schema nil))

  ([client table attr-defs key-schema options]

   (-> options
       (assoc :table table
              :attr-defs attr-defs
              :key-schema key-schema)
       (pre-process)
       (->> (client/make-request client "CreateTable"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateBackup.html
(defn create-backup

  ([client table backup]
   (create-backup client table backup nil))

  ([client table backup options]

   (-> options
       (assoc :table table :backup backup)
       (pre-process)
       (->> (client/make-request client "CreateBackup"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeBackup.html
(defn describe-backup

  ([client backup-arn]
   (describe-backup client backup-arn nil))

  ([client backup-arn options]

   (-> options
       (assoc :backup-arn backup-arn)
       (pre-process)
       (->> (client/make-request client "DescribeBackup"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html
(defn delete-table
  ([client table]
   (delete-table client table nil))

  ([client table options]

   (-> options
       (assoc :table table)
       (pre-process)
       (->> (client/make-request client "DeleteTable"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ListTables.html
(defn list-tables

  {:arglists
   '([client]
     [client
      {:keys [^Long limit
              ^String start-table]}])}

  ([client]
   (list-tables client nil))

  ([client options]

   (-> options
       (pre-process)
       (->> (client/make-request client "ListTables"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeTable.html
(defn describe-table

  ([client table]
   (describe-table client table nil))

  ([client table options]
   (-> options
       (assoc :table table)
       (pre-process)
       (->> (client/make-request client "DescribeTable"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html
(defn put-item

  {:arglists
   '([client table item]
     [client table item
      {:keys [^String sql-condition
              ^Map    attr-keys
              ^Map    attr-vals
              ^String return-consumed-capacity
              ^String return-item-collection-metrics
              ^String return-values]}])}

  ([client table item]
   (put-item client table item nil))

  ([client table item options]

   (-> options
       (assoc :table table :item item)
       (pre-process)
       (->> (client/make-request client "PutItem"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html
(defn get-item

  {:arglists
   '([client table key]
     [client table key {:keys [^List    attrs-get
                               ^Map     attr-keys
                               ^Boolean consistent-read?
                               ^String  return-consumed-capacity]}])}

  ([client table key]
   (get-item client table key nil))

  ([client table key options]

   (-> options
       (assoc :table table :key key)
       (pre-process)
       (->> (client/make-request client "GetItem"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
(defn batch-get-item

  {:arglists
   '([client table->options]
     [client table->options
      {:keys [^String return-consumed-capacity]}])}

  ([client table->options]
   (batch-get-item client table->options nil))

  ([client table->options options]

   (-> options
       (assoc :request-items table->options)
       (pre-process)
       (->> (client/make-request client "BatchGetItem"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html
(defn delete-item

  {:arglists
   '([client table key]
     [client table key
      {:keys [^String sql-condition
              ^Map    attr-keys
              ^Map    attr-vals
              ^String return-consumed-capacity
              ^String return-item-collection-metrics
              ^String return-values]}])}

  ([client table key]
   (delete-item client table key nil))

  ([client table key options]

   (-> options
       (assoc :table table :key key)
       (pre-process)
       (->> (client/make-request client "DeleteItem"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
;; https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html
(defn update-item

  {:arglists
   '([client table key]
     [client table key
      {:keys [^String sql-condition
              ^Map    attr-keys
              ^Map    attr-vals
              ^Map    set
              ^Map    add
              ^List   remove
              ^Map    delete
              ^String return-consumed-capacity
              ^String return-item-collection-metrics
              ^String return-values]}])}

  ([client table key]
   (update-item client table key nil))

  ([client table key options]

   (-> options
       (assoc :table table :key key)
       (pre-process)
       (->> (client/make-request client "UpdateItem"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html
;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
(defn query

  {:arglists
   '([client table]
     [client table
      {:keys [^Boolean consistent-read?
              ^String  sql-filter
              ^String  index
              ^Long    limit
              ^Boolean scan-forward?
              ^Map     start-key
              ^String  select
              ^Boolean return-consumed-capacity
              ^String  sql-key
              ^List    attrs-get
              ^Map     attr-keys
              ^Map     attr-vals]}])}

  ([client table]
   (query client table nil))

  ([client table options]

   (-> options
       (assoc :table table)
       (pre-process)
       (->> (client/make-request client "Query"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html
(defn scan

  {:arglists
   '([client table]
     [client table
      {:keys [^Map     attr-keys
              ^Map     attr-vals
              ^List    attrs-get
              ^Boolean consistent-read?
              ^String  sql-filter
              ^Boolean index
              ^Long    limit
              ^String  return-consumed-capacity
              ^Long    segment
              ^String  select
              ^Map     start-key
              ^Long    total-segments]}])}

  ([client table]
   (scan client table nil))

  ([client table options]

   (-> options
       (assoc :table table)
       (pre-process)
       (->> (client/make-request client "Scan"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TagResource.html
(defn tag-resource

  ([client resource-arn tags]
   (tag-resource client resource-arn tags nil))

  ([client resource-arn tags options]

   (-> options
       (assoc :tags tags :resource-arn resource-arn)
       (pre-process)
       (->> (client/make-request client "TagResource"))
       (post-process))))
