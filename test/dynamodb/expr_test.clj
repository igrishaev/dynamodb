(ns dynamodb.expr-test
  (:require
   [clojure.test :refer [is deftest]]
   [dynamodb.transform :refer [update-expression]]))


(deftest test-expr-ok

  (is (nil? (update-expression nil)))
  (is (nil? (update-expression {})))
  (is (nil? (update-expression {:set nil})))

  (is (= "SET :foo = 123"
         (update-expression
          {:set {:foo "123"}})))

  (is (= "SET :foo = 123, :bar = :test"
         (update-expression
          {:set {:foo "123" :bar :test}})))

  (is (= "SET :foo = 1 ADD :amount :one, :count two REMOVE #this, that, more DELETE :colors :red"
         (update-expression
          {:set {:foo 1}
           :add {:amount :one
                 :count "two"}
           :remove [:this "that" "more"]
           :delete {:colors :red}}))))
