(ns dynamodb.spec
  (:require
   [dynamodb.mask :as mask]
   [dynamodb.api :as api]
   [dynamodb.constant :as const]
   [clojure.string :as str]
   [clojure.spec.alpha :as s]))


(s/def ::ne-string
  (s/and string? (complement str/blank?)))


(s/def ::access-key mask/masked?)
(s/def ::secret-key mask/masked?)
(s/def ::endpoint   ::ne-string)
(s/def ::region     ::ne-string)


(s/def ::client
  (s/keys :req-un [::access-key
                   ::secret-key
                   ::endpoint
                   ::region]))


(s/def ::table ::ne-string)


(s/def ::attr
  (s/or :keyword keyword?
        :string ::ne-string))


(s/def ::item
  (s/map-of ::attr any?))


(s/def ::sql-condition ::ne-string)


(s/def ::attr-keys
  (s/map-of keyword? keyword?))


(s/def ::attr-vals
  (s/map-of keyword? any?))


(s/def ::consistent-read?
  boolean?)


(s/def ::attrs-get
  (s/coll-of (s/or :string ::ne-string
                   :keyword keyword?)))


(s/def ::return-values
  #{const/return-values-none
    const/return-values-all-old
    const/return-values-updated-old
    const/return-values-all-new
    const/return-values-updated-new})


;;
;; API
;;

(s/fdef api/delete-table
  :args
  (s/cat :client ::client
         :table ::table))


(s/fdef api/describe-table
  :args
  (s/cat :client ::client
         :table ::table))


(s/fdef api/put-item
  :args
  (s/cat :client ::client
         :table ::table
         :item ::item
         :params
         (s/?
          (s/nilable
           (s/keys
            :opt-un [::sql-condition
                     ::attr-keys
                     ::attr-vals
                     ::return-consumed-capacity
                     ::return-item-collection-metrics
                     ::return-values])))))


(s/fdef api/get-item
  :args
  (s/cat :client ::client
         :table ::table
         :key ::item
         :params
         (s/?
          (s/nilable
           (s/keys
            :opt-un [::attrs-get
                     ::attr-keys
                     ::consistent-read?
                     ::return-consumed-capacity
                     ::return-item-collection-metrics])))))


(s/fdef api/delete-item
  :args
  (s/cat :client ::client
         :table ::table
         :key ::item
         :params
         (s/?
          (s/nilable
           (s/keys
            :opt-un [::sql-condition
                     ::attrs-get
                     ::attr-keys
                     ::attr-vals
                     ::return-consumed-capacity
                     ::return-item-collection-metrics
                     ::return-values])))))
