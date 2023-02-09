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


(defn pre-process [params]

  (let [{:keys [
                add
                attr-keys
                attr-vals
                attrs-get
                backup
                backup-arn
                consistent-read?
                delete
                index
                item
                key
                keys
                limit
                remove
                request-items
                resource-arn
                return-consumed-capacity
                return-item-collection-metrics
                return-values
                scan-forward?
                segment
                select
                set
                sql-condition
                sql-filter
                sql-key
                start-key
                start-table
                table
                tags
                total-segments
                ]}
        params]

    (cond-> {}

      (some? consistent-read?)
      (assoc :ConsistentRead consistent-read?)

      sql-filter
      (assoc :FilterExpression sql-filter)

      sql-key
      (assoc :KeyConditionExpression sql-key)

      start-key
      (assoc :ExclusiveStartKey (encode-attrs start-key))

      segment
      (assoc :Segment segment)

      backup
      (assoc :BackupName backup)

      total-segments
      (assoc :TotalSegments total-segments)

      keys
      (assoc :Keys (mapv encode-attrs keys))

      tags
      (assoc :Tags (for [[k v] tags]
                     {:Key k
                      :Value v}))

      backup-arn
      (assoc :BackupArn backup-arn)

      resource-arn
      (assoc :ResourceArn resource-arn)

      request-items
      (assoc :RequestItems
             (reduce-kv
              (fn [acc table params]
                (assoc acc table (pre-process params)))
              {}
              request-items))

      (some? scan-forward?)
      (assoc :ScanIndexForward scan-forward?)

      select
      (assoc :Select select)

      index
      (assoc :IndexName index)

      (or set add remove delete)
      (as-> params'
          (let [update-expression
                (transform/update-expression
                 {:set set
                  :add add
                  :remove remove
                  :delete delete})]
            (assoc params' :UpdateExpression update-expression)))

      start-table
      (assoc :ExclusiveStartTableName start-table)

      limit
      (assoc :Limit limit)

      sql-condition
      (assoc :ConditionExpression sql-condition)

      table
      (assoc :TableName table)

      attrs-get
      (assoc :ProjectionExpression (-enc-attr-to-get attrs-get))

      item
      (assoc :Item (encode-attrs item))

      key
      (assoc :Key (encode-attrs key))

      attr-keys
      (assoc :ExpressionAttributeNames (-enc-attr-keys attr-keys))

      attr-vals
      (assoc :ExpressionAttributeValues (-enc-attr-vals attr-vals))

      return-consumed-capacity
      (assoc :ReturnConsumedCapacity return-consumed-capacity)

      return-item-collection-metrics
      (assoc :ReturnItemCollectionMetrics return-item-collection-metrics)

      return-values
      (assoc :ReturnValues return-values))))


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
(defn create-table

  ([client table attrs key-schema]
   (create-table client table attrs key-schema nil))

  ([client table attrs key-schema
    {:keys [tags
            table-class
            billing-mode
            provisioned-throughput
            GlobalSecondaryIndexes
            LocalSecondaryIndexes
            SSESpecification
            StreamSpecification]}]

   (let [AttributeDefinitions
         (for [[attr-name attr-type] attrs]
           {:AttributeName attr-name
            :AttributeType attr-type})

         KeySchema
         (for [[attr-name key-type] key-schema]
           {:AttributeName attr-name
            :KeyType key-type})

         params
         (cond-> {:AttributeDefinitions AttributeDefinitions
                  :KeySchema KeySchema
                  :TableName table}

           billing-mode
           (assoc :BillingMode billing-mode)

           provisioned-throughput
           (as-> params'
               (let [[read-units write-units]
                     provisioned-throughput]
                 (assoc params'
                        :ProvisionedThroughput
                        {:ReadCapacityUnits read-units
                         :WriteCapacityUnits write-units})))

           table-class
           (assoc :TableClass table-class)

           tags
           (assoc :Tags (for [[tag-key tag-val] tags]
                          {:Key tag-key
                           :Value tag-val}))

           SSESpecification
           (assoc :SSESpecification SSESpecification)

           GlobalSecondaryIndexes
           (assoc :GlobalSecondaryIndexes GlobalSecondaryIndexes)

           LocalSecondaryIndexes
           (assoc :LocalSecondaryIndexes LocalSecondaryIndexes)

           StreamSpecification
           (assoc :StreamSpecification StreamSpecification))]

     (client/make-request client "CreateTable" params))))


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
