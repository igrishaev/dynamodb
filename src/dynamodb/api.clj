(ns dynamodb.api
  (:import
   java.net.URI)
  (:require
   [dynamodb.constant :as const]
   [dynamodb.client :as client]
   [dynamodb.encode :refer [encode-attrs]]))


(defmacro as
  {:style/indent 1}
  [x [bind] & body]
  `(let [~bind ~x]
     ~@body))


(defn make-client

  ([access-key secret-key endpoint region]
   (make-client access-key secret-key endpoint region nil))

  ([access-key secret-key endpoint region
    {:keys [version]
     :or {version const/version-20120810}}]

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
      :region region})))


;; GetItem
;; BatchGetItem
;; BatchWriteItem
;; CreateBackup
;; CreateGlobalTable
;; DeleteBackup
;; DeleteItem
;; DescribeBackup
;; ExecuteStatement
;; ExecuteTransaction
;; PutItem
;; Scan
;; UpdateItem


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


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html#DDB-DeleteTable-request-TableName
(defn delete-table
  [client table]
  (let [params {:TableName table}]
    (client/make-request client "DeleteTable" params)))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ListTables.html
(defn list-tables
  [client {:keys [ExclusiveStartTableName
                  limit]}]

  (let [params
        (cond-> {}

          ExclusiveStartTableName
          (assoc :ExclusiveStartTableName ExclusiveStartTableName)

          limit
          (assoc :Limit limit))]

    (client/make-request client "ListTables" params)))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html
(defn put-item

  ([client table item]
   (put-item client table item nil))

  ([client table item {:keys [return]}]

   (let [params
         (cond-> {:TableName table
                  :Item (encode-attrs item)})

         response
         (client/make-request client "PutItem" params)]

     response


     #_
     (update response :Attributes item-decode))))


#_
(comment

  (def -c (make-client "123"
                       "abc"
                       "http://localhost:8000/foo"
                       "ru-central1"))

  (create-table -c "FooBar"
                [[:id :N]]
                [[:id :HASH]]
                nil
                )

  (list-tables -c)

  (put-item -c :foobar {:aaa 1 :bbb 2})



  )
