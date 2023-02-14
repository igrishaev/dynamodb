(ns dynamodb.params-test
  (:require
   [dynamodb.params :refer [set-param]]
   [dynamodb.sql :refer [sql]]
   [clojure.test :refer [is deftest]]))


(deftest test-param-set

  (let [params
        (set-param {:foo 1}
                   :set
                   {:user/id (sql "fooo(:test)")
                    "#fooo" 42})

        {:keys [UpdateExpression
                ExpressionAttributeNames
                ExpressionAttributeValues]}
        params]

    (is (= #{:user/id}
           (-> ExpressionAttributeNames
               vals
               set)))

    (is (= #{{:N "42"}}
           (-> ExpressionAttributeValues
               vals
               set)))

    (is (re-matches
         #" SET #attr\d+ = fooo\(:test\), #fooo = :value\d+"
         UpdateExpression))))


(deftest test-param-add

  (let [params
        (set-param {:foo 1}
                   :add
                   {:user/id 1
                    "#colors" #{"r" "g" "b"}})

        {:keys [UpdateExpression
                ExpressionAttributeNames
                ExpressionAttributeValues]}
        params]

    (is (= #{:user/id}
           (-> ExpressionAttributeNames
               vals
               set)))

    (is (= #{{:N "1"} {:SS #{"b" "r" "g"}}}
           (-> ExpressionAttributeValues
               vals
               set)))

    (is (re-matches
         #" ADD #attr\d+ :value\d+, #colors :value\d+"
         UpdateExpression))))


(deftest test-param-remove

  (let [params
        (set-param {:foo 1}
                   :remove
                   [:user/id "#foo" "user_name"])

        {:keys [UpdateExpression
                ExpressionAttributeNames]}
        params]

    (is (= #{:user/id}
           (-> ExpressionAttributeNames
               vals
               set)))

    (is (re-matches
         #" REMOVE #attr\d+, #foo, user_name"
         UpdateExpression))))


(deftest test-param-delete

  (let [params
        (set-param {:foo 1}
                   :delete
                   {:profile/tags #{"foo" "bar" "baz"}
                    "numbers" #{1 2 3}})

        {:keys [UpdateExpression
                ExpressionAttributeNames
                ExpressionAttributeValues]}
        params]

    (is (= #{:profile/tags}
           (-> ExpressionAttributeNames
               vals
               set)))

    (is (= #{{:NS #{"3" "1" "2"}}
             {:SS #{"foo" "bar" "baz"}}}
           (-> ExpressionAttributeValues
               vals
               set)))

    (is (re-matches
         #" DELETE #attr\d+ :value\d+, numbers :value\d+"
         UpdateExpression))))


(deftest test-unknown-param

  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Unknown parameter"
       (set-param {:foo 1}
                  :hello
                  {:aaa "test"}))))
