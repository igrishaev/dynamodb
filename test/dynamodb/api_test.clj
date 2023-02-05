(ns dynamodb.api-test
  (:require
   [dynamodb.constant :as const]
   [dynamodb.api :as api]
   [clojure.test :refer [is deftest]]))

(def PORT 8000)

(def CLIENT
  (api/make-client "public-key"
                   "secret-key"
                   (format "http://localhost:%s/foo" PORT)
                   "voronezh"))


(deftest test-client-ok
  (is (= {:path "/foo"
          :service "dynamodb"
          :access-key "public-key"
          :secret-key "secret-key"
          :region "voronezh"
          :host "localhost"
          :content-type "application/x-amz-json-1.0"
          :version "20120810"
          :endpoint "http://localhost:8000/foo"}
         CLIENT)))


(deftest test-create-table-pay-per-request

  (let [table
        (name (gensym "Table"))

        response
        (api/create-table CLIENT
                          table
                          {:user/id :N
                           :user/name :S}
                          {:user/id const/key-type-hash
                           :user/name const/key-type-range}
                          {:billing-mode const/billing-mode-pay-per-request})]

    (is (= {:TableDescription
            {:TableStatus "ACTIVE"
             :ItemCount 0
             :KeySchema
             [{:AttributeName "user/id" :KeyType "HASH"}
              {:AttributeName "user/name" :KeyType "RANGE"}]
             :ProvisionedThroughput
             {:LastIncreaseDateTime 0.0
              :LastDecreaseDateTime 0.0
              :NumberOfDecreasesToday 0
              :ReadCapacityUnits 0
              :WriteCapacityUnits 0}
             :CreationDateTime ::DUMMY
             :TableName table
             :AttributeDefinitions
             [{:AttributeName "user/id" :AttributeType "N"}
              {:AttributeName "user/name" :AttributeType "S"}]
             :TableArn (format "arn:aws:dynamodb:ddblocal:000000000000:table/%s" table)
             :TableSizeBytes 0
             :BillingModeSummary
             {:BillingMode "PAY_PER_REQUEST"
              :LastUpdateToPayPerRequestDateTime ::DUMMY}}}

           (-> response
               (assoc-in [:TableDescription :BillingModeSummary :LastUpdateToPayPerRequestDateTime] ::DUMMY)
               (assoc-in [:TableDescription :CreationDateTime] ::DUMMY))))))


(deftest test-create-table-provisioned

  (let [table
        (name (gensym "Table"))

        response
        (api/create-table CLIENT
                          table
                          {:user/id :N
                           :user/name :S}
                          {:user/id const/key-type-hash
                           :user/name const/key-type-range}
                          {:billing-mode const/billing-mode-provisioned
                           :provisioned-throughput [11 99]})]

    (is (= {:TableDescription
            {:TableStatus "ACTIVE"
             :ItemCount 0
             :KeySchema
             [{:AttributeName "user/id" :KeyType "HASH"}
              {:AttributeName "user/name" :KeyType "RANGE"}]
             :ProvisionedThroughput
             {:LastIncreaseDateTime 0.0
              :LastDecreaseDateTime 0.0
              :NumberOfDecreasesToday 0
              :ReadCapacityUnits 11
              :WriteCapacityUnits 99}
             :CreationDateTime ::DUMMY
             :TableName table
             :AttributeDefinitions
             [{:AttributeName "user/id" :AttributeType "N"}
              {:AttributeName "user/name" :AttributeType "S"}]
             :TableArn (format "arn:aws:dynamodb:ddblocal:000000000000:table/%s" table)
             :TableSizeBytes 0
             :BillingModeSummary
             {:LastUpdateToPayPerRequestDateTime ::DUMMY}}}

           (-> response
               (assoc-in [:TableDescription :BillingModeSummary :LastUpdateToPayPerRequestDateTime] ::DUMMY)
               (assoc-in [:TableDescription :CreationDateTime] ::DUMMY))))))
