(ns dynamodb.decode-test
  (:import
   java.util.UUID)
  (:require
   [dynamodb.decode :refer [decode
                            decode-attrs]]
   [clojure.test :refer [is deftest]]))


(deftest test-decode-ok

  (is (= "this text is base64-encoded"
         (decode {:B "dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk"})))


  (is (= false
         (decode {:BOOL false})))

  (is (= #{"Snowy" "Rainy" "Sunny"}
         (decode {:BS ["U3Vubnk=", "UmFpbnk=", "U25vd3k="]})))

  (is (= ["Cookies" "Coffee" 3.14159]
         (decode {:L [ {"S" "Cookies"} {"S" "Coffee"} {"N" "3.14159"}]})))

  (is (= {"Name" "Joe" "Age" 35}
         (decode {:M {"Name" {"S" "Joe"}
                      "Age" {"N" "35"}}})))

  (is (= -99.33
         (decode {:N "-99.33"})))

  (is (= 1.1111111111111111E24
         (decode {:N "1111111111111111111111111.1111111111111111111111111"})))

  (is (= #{"3.14" "7.5" "42.2" "-19"}
         (decode {:NS ["42.2", "-19", "7.5", "3.14"]})))

  (is (= nil
         (decode {:NULL true})))

  (is (= "hello"
         (decode {:S "hello"})))

  (is (= #{"Giraffe", "Hippo" ,"Zebra"}
         (decode {:SS ["Giraffe", "Hippo" ,"Zebra"]})))

  (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"(?i)Cannot decode value"
        (decode {:AA 42}))))


(deftest test-decode-attrs
  (is (= {:foo 1 :bar {:a 1}}
         (decode-attrs {:foo {:N 1}
                        :bar {:M {:a {:N 1}}}}))))
