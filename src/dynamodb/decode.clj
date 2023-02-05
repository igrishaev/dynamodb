(ns dynamodb.decode
  "
  https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html
  "
  (:require
   [cheshire.core :as json]
   [dynamodb.util :as util]
   [dynamodb.codec :as codec]))


(defn decode-bytes [string]
  (-> string
      (codec/str->bytes "utf-8")
      (codec/b64-decode)
      (codec/bytes->str "utf-8")))


(defn decode [mapping]
  (let [k (-> mapping first key)
        v (-> mapping first val)]

    (case k

      (:B "B") ;; "dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk"
      (decode-bytes v)

      (:BOOL "BOOL") ;; true
      v

      (:BS "BS") ;; ["U3Vubnk=", "UmFpbnk=", "U25vd3k="]
      (set (map decode-bytes v))

      (:L "L") ;; [ {"S": "Cookies"} , {"S": "Coffee"}, {"N": "3.14159"}]
      (mapv decode v)

      (:M "M") ;; {"Name": {"S": "Joe"}, "Age": {"N": "35"}}
      (util/update-vals v decode)

      (:N "N") ;; "123.45"
      (json/parse-string v)

      (:NS "NS") ;; ["42.2", "-19", "7.5", "3.14"]
      (set v)

      (:NULL "NULL") ;; true
      nil

      (:S "S") ;; "Hello"
      v

      (:SS "SS") ;; ["Giraffe", "Hippo" ,"Zebra"]
      (set v)

      ;; else
      (throw
       (ex-info
        (format "Cannot decode value, tag %s" k)
        {:tag k
         :value v})))))


(defn decode-attrs [mapping]
  (util/update-vals mapping decode))
