(ns dynamodb.api
  (:import
   java.net.URI)
  (:require
   [dynamodb.client :as client]
   [dynamodb.encode :refer [encode-attrs]]))


(defmacro as
  {:style/indent 1}
  [x [bind] & body]
  `(let [~bind ~x]
     ~@body))


(defn make-client
  [access-key
   secret-key
   endpoint
   region]

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
     :version "20120810"
     :region region}))



(defn create-table
  "https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html"

  [client table attrs key-schema

   {:keys [tags
           billing-mode
           table-class
           GlobalSecondaryIndexes
           LocalSecondaryIndexes
           ProvisionedThroughput
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

          GlobalSecondaryIndexes
          (assoc :GlobalSecondaryIndexes GlobalSecondaryIndexes)

          LocalSecondaryIndexes
          (assoc :LocalSecondaryIndexes LocalSecondaryIndexes)

          ProvisionedThroughput
          (assoc :ProvisionedThroughput ProvisionedThroughput)

          SSESpecification
          (assoc :SSESpecification SSESpecification)

          StreamSpecification
          (assoc :StreamSpecification StreamSpecification)

          table-class
          (assoc :TableClass table-class)

          tags
          (assoc :Tags (for [[tag-key tag-val] tags]
                         {:Key tag-key
                          :Value tag-val})))

        response
        (client/make-request client "CreateTable" params)]

    response))


(defn list-tables
  [client {:keys [ExclusiveStartTableName
                  limit]}]

  (let [params
        (cond-> {}

          ExclusiveStartTableName
          (assoc :ExclusiveStartTableName ExclusiveStartTableName)

          limit
          (assoc :Limit limit))

        response
        (client/make-request client "ListTables" params)]

    response))


(defn put-item
  "
  https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html
  "
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
