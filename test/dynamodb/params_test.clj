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
         #" SET #attr\d+ = fooo\(:test\),  #fooo = :value\d+"
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
         #" ADD #attr\d+ :value\d+,  #colors :value\d+"
         UpdateExpression))))
