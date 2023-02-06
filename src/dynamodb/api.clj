(ns dynamodb.api
  "
  BatchExecuteStatement
  BatchGetItem
  BatchWriteItem
  CreateBackup
  CreateGlobalTable
+ CreateTable
  DeleteBackup
  DeleteItem
+ DeleteTable
  DescribeBackup
  DescribeContinuousBackups
  DescribeContributorInsights
  DescribeEndpoints
  DescribeExport
  DescribeGlobalTable
  DescribeGlobalTableSettings
  DescribeImport
  DescribeKinesisStreamingDestination
  DescribeLimits
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
  ListBackups
  ListContributorInsights
  ListExports
  ListGlobalTables
  ListImports
+ ListTables
  ListTagsOfResource
+ PutItem
  Query
  RestoreTableFromBackup
  RestoreTableToPointInTime
  Scan
  TagResource
  TransactGetItems
  TransactWriteItems
  UntagResource
  UpdateContinuousBackups
  UpdateContributorInsights
  UpdateGlobalTable
  UpdateGlobalTableSettings
  UpdateItem
  UpdateTable
  UpdateTableReplicaAutoScaling
  UpdateTimeToLive
  "
  (:import
   java.net.URI)
  (:require
   [clojure.string :as str]
   [dynamodb.util :as util :refer [as]]
   [dynamodb.transform :as transform]
   [dynamodb.constant :as const]
   [dynamodb.client :as client]
   [dynamodb.decode :refer [decode-attrs]]
   [dynamodb.encode :refer [encode-attrs]]))


(defn make-client

  ([access-key secret-key endpoint region]
   (make-client access-key secret-key endpoint region nil))

  ([access-key secret-key endpoint region
    {:keys [async?
            version]
     :or {async? false
          version const/version-20120810}}]

   (let [uri
         (new URI endpoint)

         host
         (.getHost uri)

         path
         (.getPath uri)]

     {:access-key access-key
      :secret-key secret-key
      :endpoint endpoint
      :content-type "application/x-amz-json-1.0"
      :host host
      :path path
      :service "dynamodb"
      :version version
      :region region
      :async? async?})))


;; BatchGetItem
;; DeleteItem
;; UpdateItem
;; Query
;; Scan
;; BatchWriteItem

;; CreateBackup
;; CreateGlobalTable
;; DeleteBackup
;; DescribeBackup
;; ExecuteStatement
;; ExecuteTransaction


;; ok
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
           (as [params']
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


;; ok
;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html#DDB-DeleteTable-request-TableName
(defn delete-table
  [client table]
  (let [params {:TableName table}]
    (client/make-request client "DeleteTable" params)))


;; ok
;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ListTables.html
(defn list-tables

  ([client]
   (list-tables client nil))

  ([client {:keys [limit
                   last-table]}]
   (let [params
         (cond-> {}

           last-table
           (assoc :ExclusiveStartTableName last-table)

           limit
           (assoc :Limit limit))]

     (client/make-request client "ListTables" params))))


;; ok
;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeTable.html
(defn describe-table
  [client table]
  (let [params {:TableName table}]
    (client/make-request client "DescribeTable" params)))


;; ok
;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html
(defn put-item

  ([client table item]
   (put-item client table item nil))

  ([client table item {:keys [condition
                              attr-names
                              attr-values
                              return-consumed-capacity
                              return-item-collection-metrics
                              return-values]}]

   (let [params
         (cond-> {:TableName table
                  :Item (encode-attrs item)}

           condition
           (assoc :ConditionExpression condition)

           attr-names
           (assoc :ExpressionAttributeNames attr-names)

           attr-values
           (assoc :ExpressionAttributeValues
                  (-> attr-values
                      (encode-attrs)
                      (util/update-keys transform/key->attr-placeholder)))

           return-consumed-capacity
           (assoc :ReturnConsumedCapacity return-consumed-capacity)

           return-item-collection-metrics
           (assoc :ReturnItemCollectionMetrics return-item-collection-metrics)

           return-values
           (assoc :ReturnValues return-values))

         response
         (client/make-request client "PutItem" params)]

     (if (:Attributes response)
       (update response :Attributes decode-attrs)
       response))))


;; ok
;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html
(defn get-item

  ([client table pk]
   (get-item client table pk nil))

  ([client table pk {:keys [attrs
                            attr-names
                            consistent-read?
                            return-consumed-capacity]}]

   (let [params
         (cond-> {:TableName table
                  :Key (encode-attrs pk)}

           attr-names
           (assoc :ExpressionAttributeNames attr-names)

           (some? consistent-read?)
           (assoc :ConsistentRead consistent-read?)

           return-consumed-capacity
           (assoc :ReturnConsumedCapacity return-consumed-capacity)

           attrs
           (assoc :ProjectionExpression
                  (->> attrs
                       (map transform/key->proj-expr)
                       (str/join ", "))))

         response
         (client/make-request client "GetItem" params)]

     (cond

       (= response {})
       nil

       (:Item response)
       (update response :Item decode-attrs)

       :else
       response))))


(defn delete-item

  ([client table pk]
   (delete-item client table pk nil))

  ([client table pk {:keys [condition
                            attr-names
                            attr-values
                            ReturnConsumedCapacity
                            ReturnItemCollectionMetrics
                            ReturnValues]}]

   (let [params
         (cond-> {:TableName table
                  :Key (encode-attrs pk)}

           condition
           (assoc :ConditionExpression condition)

           attr-names
           (assoc :ExpressionAttributeNames attr-names)

           attr-values
           (assoc :ExpressionAttributeValues
                  (-> attr-values
                      (encode-attrs)
                      (util/update-keys transform/key->attr-placeholder))))

         response
         (client/make-request client "Deletetem" params)

         ]

     response)))


#_
(comment

  (def -c (make-client "123"
                       "abc"
                       "http://localhost:8000/foo"
                       "ru-central1"
                       {:async? true}))

  (create-table -c "FooBar"
                [[:id :N]]
                [[:id :HASH]]
                nil
                )

  (list-tables -c)

  (put-item -c :foobar {:aaa 1 :bbb 2})



  )
