(ns dynamodb.encode-test
  (:import
   java.util.UUID)
  (:require
   [dynamodb.encode :refer [encode
                            encode-attrs]]
   [clojure.test :refer [is deftest]]))


(deftest test-encode-ok

  (is (= {:N 0}
         (encode 0)))

  (is (= {:S "hello"}
         (encode "hello")))

  (is (= {:BOOL false}
         (encode false)))

  (is (= {:B "AAECAwQ="}
         (encode (byte-array [0 1 2 3 4]))))

  (is (= {:BS #{"BAUG" "AQID" "BwgJ"}}
         (-> (encode #{(byte-array [1 2 3])
                       (byte-array [4 5 6])
                       (byte-array [7 8 9])})
             (update :BS set))))

  (is (= {:L [{:N 1} {:S "two"} {:BOOL false} {:NULL true}]}
         (encode (list 1 "two" false nil))))

  (is (= {:L [{:N 1} {:S "two"} {:BOOL false} {:NULL true}]}
         (encode [1 "two" false nil])))

  (is (= {:M
          {"foo" {:N 1}
           "bar" {:BOOL true}
           "olo"
           {:M {"test1" {:L [{:N 1} {:N 2} {:N 3}]}
                "test2" {:NULL true}}}}}

         (encode {"foo" 1
                  "bar" true
                  "olo" {"test1" [1 2 3]
                         "test2" nil}})))

  (is (= {:M {:foo/bar {:NS #{1 3 2}}
              :bar/foo {:S "hello"}}}
         (encode {:foo/bar #{1 2 3}
                  :bar/foo "hello"})))

  (is (= {:L [{:S "foo"}
              {:S "bar"}
              {:S "test/hello"}]}
         (encode [:foo :bar :test/hello])))

  (is (= {:L [{:S "foo"}
              {:S "bar"}
              {:S "test/hello"}]}
         (encode ['foo 'bar 'test/hello])))

  (is (= {:NS #{4 6 5}}
         (encode #{4 5 6})))

  (is (= {:SS #{"a" "b" "c"}}
         (encode #{"a" "b" "c"})))

  (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"(?i)Cannot encode a set"
        (encode #{nil 1 2})))

  (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"(?i)Cannot encode a set"
        (encode #{true false})))

  (let [uuid
        (UUID/randomUUID)]
    (is (= {:L [{:S (str uuid)}]}
           (encode [uuid])))))


(deftest test-encode-attrs

  (is (= {:foo/test1 {:N 1}
          :bar/test2 {:L [{:S "hello"}]}}
         (encode-attrs {:foo/test1 1
                        :bar/test2 ["hello"]}))))
