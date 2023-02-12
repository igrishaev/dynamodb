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

    (is (re-matches
         #" SET #attr\d+ = fooo\(:test\),  #fooo = :value\d+"
         UpdateExpression))

    (is ExpressionAttributeNames)
    (is ExpressionAttributeValues)))


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

    (is (= 1 params))

    (is (re-matches
         #" SET #attr\d+ = fooo\(:test\),  #fooo = :value\d+"
         UpdateExpression))

    (is ExpressionAttributeNames)
    (is ExpressionAttributeValues)))
