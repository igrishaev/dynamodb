(ns dynamodb.api
  (:import
   java.net.URI)
  (:require
   [dynamodb.client :as client]
   [dynamodb.encode :refer [encode-attrs]]))


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


(defn list-tables
  [client {:keys [ExclusiveStartTableName
                  Limit]}]

  (let [params
         (cond-> {}

           ExclusiveStartTableName
           (assoc :ExclusiveStartTableName ExclusiveStartTableName)

           Limit
           (assoc :Limit Limit))

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

  (list-tables -c)

  (put-item -c :foobar {:aaa 1 :bbb 2})



  )
