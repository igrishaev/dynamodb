(ns dynamodb.api-test
  (:import
   java.util.UUID)
  (:require
   [clojure.spec.test.alpha :as spec.test]
   [clojure.test :refer [is deftest]]
   [dynamodb.api :as api]
   [dynamodb.constant :as const]
   [dynamodb.mask :as mask]
   dynamodb.spec))


(spec.test/instrument)


(def PORT 8000)

(def CLIENT
  (api/make-client "public-key"
                   "secret-key"
                   (format "http://localhost:%s/foo" PORT)
                   "voronezh"))


(defn make-table-name []
  (format "Table-%s" (UUID/randomUUID)))


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
          :region "voronezh"
          :host "localhost"
          :content-type "application/x-amz-json-1.0"
          :version "20120810"
          :endpoint "http://localhost:8000/foo"
          :throw? false}
         (dissoc CLIENT :access-key :secret-key)))

  (is (-> CLIENT :access-key mask/masked?))
  (is (-> CLIENT :secret-key mask/masked?))

  (is (= "<< masked >>"
         (pr-str (:secret-key CLIENT)))))


(deftest test-create-table-pay-per-request

  (let [table
        (make-table-name)

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
        (make-table-name)

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


(deftest test-create-table-full-params

  (let [table
        (make-table-name)

        response
        (api/create-table CLIENT
                          table
                          {:user/id :N
                           :user/name :S}
                          {:user/id const/key-type-hash
                           :user/name const/key-type-range}
                          {:tags {:foo "hello"}
                           :table-class const/table-class-standard
                           :billing-mode const/billing-mode-pay-per-request
                           :provisioned-throughput [111 222]
                           :global-indexes {:idxa {:key-schema {:user/id const/key-type-hash}
                                                   :projection {:non-key-attrs [:aaa :bbb]
                                                                :type const/projection-type-include}
                                                   :provisioned-throughput [111 222]}
                                            :idxb {:key-schema {:user/id const/key-type-hash}
                                                   :projection {:non-key-attrs [:aaa :bbb]
                                                                :type const/projection-type-include}
                                                   :provisioned-throughput [111 222]}}
                           :local-indexes {:idxc {:key-schema {:user/id const/key-type-hash
                                                               :user/name const/key-type-range}
                                                  :projection {:non-key-attrs [:ccc :ddd]
                                                               :type const/projection-type-include}}}
                           :stream-spec {:enabled? true
                                         :view-type const/stream-view-type-new-and-old-images}
                           :sse-spec {:enabled? true
                                      :kms-key-id "key:abc"
                                      :type const/sse-type-aes256}})]

    ;; the result is huge
    (is (:TableDescription response))))


(deftest test-api-negative-response

  (let [table
        (make-table-name)

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
        (make-table-name)

        _
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
                          :start-table LastEvaluatedTableName})]

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

    (is (nil? resp1))

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
                      {:sql-condition "#foo in (:one, :two, :three)"
                       :attr-keys {"#foo" :user/foo}
                       :attr-vals {":one" 1
                                   ":two" 2
                                   ":three" 3}
                       :return-values const/return-values-all-old})

        resp3
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"}
                      {:return-values const/return-values-all-old})]

    (is (nil? resp1))

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

        _
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"
                       :user/foo 1})

        _
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"
                       :user/foo 3}
                      {:sql-condition "#foo in (:ten, :eleven)"
                       :attr-keys {"#foo" :user/foo}
                       :attr-vals {":ten" 10
                                   ":eleven" 11}})

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
                      {:attrs-get [:test/kek "bar.baz[1]" "abc" "foo"]})]

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
                         {:sql-condition "#kek in (:foo, :bar, :baz)"
                          :attr-keys {"#kek" :test/kek}
                          :attr-vals {":foo" 1
                                      ":bar" 99
                                      ":baz" 3}})

        resp3
        (api/get-item CLIENT
                      table
                      {:user/id 1 :user/name "Ivan"})]

    (is (nil? resp2))
    (is (nil? resp3))))


(deftest test-delete-item-condition-fails

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
                         {:sql-condition "#kek in (:foo, :bar, :baz)"
                          :attr-keys {"#kek" :test/kek}
                          :attr-vals {":foo" 1
                                      ":bar" 2
                                      ":baz" 3}})

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
             :Key #:user{:id {:N "1"} :name {:S "Ivan"}}
             :ConditionExpression "#kek in (:foo, :bar, :baz)"
             :ExpressionAttributeNames {"#kek" :test/kek}
             :ExpressionAttributeValues
             {":foo" {:N "1"} ":bar" {:N "2"} ":baz" {:N "3"}}}
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
                       :test/lol "poo"
                       :abc "test"
                       :amount 3
                       :kek/numbers #{1 3 5}})

        resp2
        (api/update-item CLIENT
                         table
                         {:user/id 1
                          :user/name "Ivan"}
                         {:sql-condition "#id = :one"
                          :attr-keys {"#id" :user/id
                                      "#kek" :test/kek
                                      "#numbers" :kek/numbers
                                      "#lol" :test/lol}
                          :attr-vals {":num" 123
                                      ":one" 1
                                      ":drop" #{1 5}}
                          :set {"Foobar" :num}
                          :add {"amount" :one}
                          :delete {:numbers :drop}
                          :remove ["#kek" "abc" :lol]})

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


(deftest test-query-ok

  (let [table
        (make-table-name)

        _
        (make-tmp-table table)

        _
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"
                       :test/foo 1})

        _
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Juan"
                       :test/foo 2})

        _
        (api/put-item CLIENT
                      table
                      {:user/id 2
                       :user/name "Huan"
                       :test/foo 3})

        params
        {:sql-key "#id = :one"
         :attr-keys {"#id" :user/id}
         :attr-vals {":one" 1}
         :limit 1}

        resp1
        (api/query CLIENT table params)

        {:keys [LastEvaluatedKey]}
        resp1

        resp2
        (api/query CLIENT table
                   (assoc params :start-key LastEvaluatedKey))

        {:keys [LastEvaluatedKey]}
        resp2

        resp3
        (api/query CLIENT table
                   (assoc params :start-key LastEvaluatedKey))]

    (is (= {:Items [{:user/id 1
                     :test/foo 1
                     :user/name "Ivan"}]
            :Count 1
            :ScannedCount 1
            :LastEvaluatedKey #:user{:id 1
                                     :name "Ivan"}}

           resp1))

    (is (= {:Items [{:user/id 1
                     :test/foo 2
                     :user/name "Juan"}]
            :Count 1
            :ScannedCount 1
            :LastEvaluatedKey #:user{:id 1 :name "Juan"}}

           resp2))

    (is (= {:Items [] :Count 0 :ScannedCount 0}
           resp3))))


(deftest test-scan-ok

  (let [table
        (make-table-name)

        _
        (make-tmp-table table)

        _
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"
                       :test/foo 2})

        _
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Juan"
                       :test/foo 2})

        _
        (api/put-item CLIENT
                      table
                      {:user/id 2
                       :user/name "Huan"
                       :test/foo 3})

        params
        {:sql-filter "#foo = :two"
         :attrs-get [:test/foo "#name"]
         :attr-keys {"#foo" :test/foo
                     "#name" :user/name}
         :attr-vals {":two" 2}
         :limit 2}

        resp1
        (api/scan CLIENT table params)

        {:keys [LastEvaluatedKey]}
        resp1

        resp2
        (api/scan CLIENT table
                  (assoc params :start-key LastEvaluatedKey))]


    (is (= {:Items [{:test/foo 2
                     :user/name "Ivan"}]
            :Count 1
            :ScannedCount 2
            :LastEvaluatedKey #:user{:id 1
                                     :name "Ivan"}}

           resp1))

    (is (= {:Items [{:test/foo 2
                     :user/name "Juan"}]
            :Count 1
            :ScannedCount 1}

           resp2))))


(deftest test-batch-get-item

  (let [table1
        (make-table-name)

        table2
        (make-table-name)

        _
        (make-tmp-table table1)

        _
        (make-tmp-table table2)

        _
        (api/put-item CLIENT
                      table1
                      {:user/id 1
                       :user/name "Ivan"
                       :test/foo "foo"})

        _
        (api/put-item CLIENT
                      table2
                      {:user/id 2
                       :user/name "Huan"
                       :test/foo "bar"})

        resp
        (api/batch-get-item CLIENT
                            {table1 {:keys [{:user/id 1
                                             :user/name "Ivan"}]
                                     :attrs-get [:user/id :test/foo]}
                             table2 {:keys [{:user/id 2
                                             :user/name "Huan"}
                                            {:user/id 99
                                             :user/name "Foo"}]
                                     :attrs-get [:user/name :test/foo]}})]

    (is (= {:Responses
            {(keyword table1)
             [{:user/id 1
               :test/foo "foo"}]
             (keyword table2)
             [{:test/foo "bar"
               :user/name "Huan"}]}
            :UnprocessedKeys {}}

           resp))))


(deftest test-create-backup

  (let [table
        (make-table-name)

        _
        (make-tmp-table table)

        _
        (api/put-item CLIENT
                      table
                      {:user/id 1
                       :user/name "Ivan"
                       :test/foo "foo"})

        resp
        (api/create-backup CLIENT table "aaa")]

    (is (= {:error? true
            :status 400
            :path "com.amazonaws.dynamodb.v20120810"
            :exception "UnknownOperationException"
            :message "An unknown operation was requested."
            :payload
            {:BackupName "aaa"
             :TableName table}
            :target "CreateBackup"}
           resp))))


(deftest test-describe-backup
  (let [resp
        (api/describe-backup CLIENT "foobar")]
    (is (= {:error? true
            :status 400
            :path "com.amazonaws.dynamodb.v20120810"
            :exception "UnknownOperationException"
            :message "An unknown operation was requested."
            :payload {:BackupArn "foobar"}
            :target "DescribeBackup"}
           resp))))


(deftest test-throw-flag

  (let [table
        (make-table-name)

        _
        (make-tmp-table table)

        client
        (assoc CLIENT :throw? true)]

    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"DynamoDB failure"
         (api/create-backup client table "aaa")))))


(deftest test-tag-resource

  (let [table
        (make-table-name)

        resp1
        (make-tmp-table table)

        arn
        (-> resp1 :TableDescription :TableArn)

        resp2
        (api/tag-resource CLIENT
                          arn
                          {:aaa/foo "test"
                           :bbb/test "some/keyword"
                           "hello" "3"})]

    (is (= {:error? true
            :status 400
            :path "com.amazonaws.dynamodb.v20120810"
            :exception "UnknownOperationException"
            :message "Tagging is not currently supported in DynamoDB Local."
            :payload
            {:Tags
             #{{:Key :aaa/foo :Value "test"}
               {:Key :bbb/test :Value "some/keyword"}
               {:Key "hello" :Value "3"}}
             :ResourceArn ::DUMMY}
            :target "TagResource"}

           (-> resp2
               (assoc-in [:payload :ResourceArn] ::DUMMY)
               (update-in [:payload :Tags] set))))))
