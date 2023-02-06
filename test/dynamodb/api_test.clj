(ns dynamodb.api-test
  (:require
   dynamodb.spec
   [dynamodb.constant :as const]
   [dynamodb.api :as api]
   [clojure.spec.test.alpha :as spec.test]
   [clojure.test :refer [is deftest]]))


(spec.test/instrument `api/delete-table
                      `api/put-item)


(def PORT 8000)

(def CLIENT
  (api/make-client "public-key"
                   "secret-key"
                   (format "http://localhost:%s/foo" PORT)
                   "voronezh"))


(defn make-table-name []
  (name (gensym "Table")))


(defn make-tmp-table [table]
  (api/create-table CLIENT
                    table
                    {:user/id :N
                     :user/name :S}
                    {:user/id const/key-type-hash
                     :user/name const/key-type-range}
                    {:billing-mode const/billing-mode-pay-per-request}))


(deftest test-client-ok
  (is (= {:path "/foo"
          :service "dynamodb"
          :access-key "public-key"
          :secret-key "secret-key"
          :region "voronezh"
          :host "localhost"
          :content-type "application/x-amz-json-1.0"
          :version "20120810"
          :endpoint "http://localhost:8000/foo"
          :async? false}
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


(deftest test-api-negative-response

  (let [table
        (name (gensym "Table"))

        response
        (api/delete-table CLIENT table)]

    (is (= {:error? true
            :status 400
            :path "com.amazonaws.dynamodb.v20120810"
            :exception "ResourceNotFoundException"
            :message "Cannot do operations on a non-existent table"
            :payload {:TableName table}
            :target "DeleteTable"}
           response))))


(deftest test-api-delete-table

  (let [table
        (name (gensym "Table"))

        resp1
        (api/create-table CLIENT
                          table
                          {:user/id :N}
                          {:user/id const/key-type-hash}
                          {:billing-mode const/billing-mode-pay-per-request})

        resp2
        (api/delete-table CLIENT table)]

    (is (= {:TableDescription
            {:TableStatus "ACTIVE"
             :ItemCount 0
             :KeySchema [{:AttributeName "user/id" :KeyType "HASH"}]
             :ProvisionedThroughput
             {:LastIncreaseDateTime 0.0
              :LastDecreaseDateTime 0.0
              :NumberOfDecreasesToday 0
              :ReadCapacityUnits 0
              :WriteCapacityUnits 0}
             :CreationDateTime nil
             :TableName table
             :AttributeDefinitions
             [{:AttributeName "user/id" :AttributeType "N"}]
             :TableArn (format "arn:aws:dynamodb:ddblocal:000000000000:table/%s" table)
             :TableSizeBytes 0
             :BillingModeSummary
             {:BillingMode "PAY_PER_REQUEST"
              :LastUpdateToPayPerRequestDateTime nil}}}

           (-> resp2
               (assoc-in [:TableDescription :BillingModeSummary :LastUpdateToPayPerRequestDateTime] nil)
               (assoc-in [:TableDescription :CreationDateTime] nil))))))


(deftest test-api-list-tables

  (let [tab1
        (make-table-name)

        tab2
        (make-table-name)

        tab3
        (make-table-name)

        _
        (make-tmp-table tab1)

        _
        (make-tmp-table tab2)

        _
        (make-tmp-table tab3)

        resp1
        (api/list-tables CLIENT {:limit 1})

        {:keys [LastEvaluatedTableName]}
        resp1

        resp2
        (api/list-tables CLIENT
                         {:limit 2
                          :last-table LastEvaluatedTableName})]

    (let [{:keys [TableNames]}
          resp1]

      (is (= 1 (count TableNames))))

    (let [{:keys [TableNames]}
          resp2]

      (is (= 2 (count TableNames))))))


(deftest test-describe-table

  (let [table
        (make-table-name)

        _
        (make-tmp-table table)

        response
        (api/describe-table CLIENT table)]

    (is (= {:TableStatus "ACTIVE"
            :TableName table}
           (-> response
               :Table
               (select-keys [:TableStatus :TableName]))))))


(deftest test-put-item-ok

  (let [table
        (make-table-name)

        _
        (make-tmp-table table)

        resp1
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"
                       :foo/extra [1 true nil "kek"]}
                      {:return-values const/return-values-none})

        resp2
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"}
                      {:return-values const/return-values-all-old})]

    (is (= {} resp1))

    (is (= {:Attributes
            {:user/id 1
             :user/name "Ivan"
             :foo/extra [1 true nil "kek"]}}
           resp2))))


(deftest test-put-item-cond-expr-true

  (let [table
        (make-table-name)

        _
        (make-tmp-table table)

        resp1
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"
                       :user/foo 1}
                      {:return-values const/return-values-none})

        resp2
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"
                       :user/test 3}
                      {:condition "#foo in (:one, :two, :three)"
                       :attr-names {"#foo" :user/foo}
                       :attr-values {":one" 1
                                     :two 2
                                     ":three" 3}
                       :return-values const/return-values-all-old})

        resp3
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"}
                      {:return-values const/return-values-all-old})]

    (is (= {} resp1))

    (is (= {:Attributes #:user{:id 1
                               :name "Ivan"
                               :foo 1}}
           resp2))

    (is (= {:Attributes #:user{:id 1
                               :name "Ivan"
                               :test 3}}
           resp3))))


(deftest test-put-item-cond-expr-false

  (let [table
        (make-table-name)

        _
        (make-tmp-table table)

        resp1
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"
                       :user/foo 1})

        resp2
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"
                       :user/foo 3}
                      {:condition "#foo in (:ten, :eleven)"
                       :attr-names {"#foo" :user/foo}
                       :attr-values {:ten 10
                                     :eleven 11}})

        resp3
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"}
                      {:return-values const/return-values-all-old})]

    (is (= {:Attributes #:user{:id 1
                               :name "Ivan"
                               :foo 1}}
           resp3))))


(deftest test-get-item-ok

  (let [table
        (make-table-name)

        _
        (make-tmp-table table)

        _
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"
                       :user/foo 1})

        _
        (api/put-item CLIENT
                      table
                      {:user/id 2
                       :user/name "Huan"
                       :user/foo 2})

        resp1
        (api/get-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"})

        resp2
        (api/get-item CLIENT
                      table
                      {:user/id 3
                       :user/name "Ivan"})]

    (is (= {:Item #:user{:id 1
                         :name "Ivan"
                         :foo 1}}
           resp1))

    (is (nil? resp2))))


(deftest test-get-item-proj-expression

  (let [table
        (make-table-name)

        _
        (make-tmp-table table)

        _
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"
                       :test/kek "123"
                       :test/foo 1
                       :abc nil
                       :foo "lol"
                       :bar {:baz [1 2 3]}})

        resp1
        (api/get-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"}
                      {:attr-names {"#kek" :test/kek}
                       :attrs ["#kek" "bar.baz[1]" "abc" :foo]})] ;; TODO

    (is (= {:Item {:test/kek "123"
                   :bar {:baz [2]}
                   :abc nil
                   :foo "lol"}}
           resp1))))


(deftest test-delete-item-ok

  (let [table
        (make-table-name)

        _
        (make-tmp-table table)

        _
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"
                       :test/kek "123"})

        resp2
        (api/delete-item CLIENT
                         table
                         {:user/id 1 :user/name "Ivan"}
                         {:return-values const/return-values-all-old})

        resp3
        (api/get-item CLIENT
                      table
                      {:user/id 1 :user/name "Ivan"})]

    (is (= {:Attributes {:test/kek "123"
                         :user/id 1
                         :user/name "Ivan"}}
           resp2))

    (is (nil? resp3))))


(deftest test-delete-item-condition-ok

  (let [table
        (make-table-name)

        _
        (make-tmp-table table)

        _
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"
                       :test/kek 99})

        resp2
        (api/delete-item CLIENT
                         table
                         {:user/id 1 :user/name "Ivan"}
                         {:condition "#kek in (:foo, :bar, :baz)"
                          :attr-names {"#kek" :test/kek}
                          :attr-values {:foo 1
                                        :bar 99
                                        :baz 3}})

        resp3
        (api/get-item CLIENT
                      table
                      {:user/id 1 :user/name "Ivan"})]

    (is (nil? resp2))
    (is (nil? resp3))))


(deftest test-delete-item-condition-failes

  (let [table
        (make-table-name)

        _
        (make-tmp-table table)

        _
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"
                       :test/kek 99})

        resp2
        (api/delete-item CLIENT
                         table
                         {:user/id 1 :user/name "Ivan"}
                         {:condition "#kek in (:foo, :bar, :baz)"
                          :attr-names {"#kek" :test/kek}
                          :attr-values {:foo 1
                                        :bar 2
                                        :baz 3}})

        resp3
        (api/get-item CLIENT
                      table
                      {:user/id 1 :user/name "Ivan"})]

    (is (= {:error? true
            :status 400
            :path "com.amazonaws.dynamodb.v20120810"
            :exception "ConditionalCheckFailedException"
            :message "The conditional request failed"
            :payload
            {:TableName table
             :Key #:user{:id {:N 1} :name {:S "Ivan"}}
             :ConditionExpression "#kek in (:foo, :bar, :baz)"
             :ExpressionAttributeNames {"#kek" :test/kek}
             :ExpressionAttributeValues
             {":foo" {:N 1} ":bar" {:N 2} ":baz" {:N 3}}}
            :target "DeleteItem"}

           resp2))

    (is (= {:Item {:test/kek 99 :user/id 1 :user/name "Ivan"}}
           resp3))))


(deftest test-update-item-ok

  (let [table
        (make-table-name)

        _
        (make-tmp-table table)

        _
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"
                       :test/kek 99
                       :abc "test"
                       :amount 3
                       :kek/numbers #{1 3 5}})

        resp2
        (api/update-item CLIENT
                         table
                         {:user/id 1
                          :user/name "Ivan"}
                         {:condition "#id = :one"
                          :attr-names {"#id" :user/id
                                       "#kek" :test/kek
                                       "#numbers" :kek/numbers}
                          :attr-values {:num 123
                                        :one 1
                                        :drop #{1 5}}
                          :set {"Foobar" :num}
                          :add {"amount" :one}
                          :delete {"#numbers" :drop}
                          :remove ["#kek" "abc"]}) ;; TODO: keyword

        resp3
        (api/get-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"})]

    (is (= nil
           resp2))

    (is (= {:Item
            {:kek/numbers #{3}
             :amount 4
             :user/name "Ivan"
             :user/id 1
             :Foobar 123}}
           resp3))))
