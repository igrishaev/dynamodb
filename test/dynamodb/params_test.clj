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
