(ns dynamodb.spec
  (:require
   [dynamodb.api :as api]
   [dynamodb.mask :as mask]
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
                   ::region]
          :opt-un [::throw?]))


(s/def ::table
  ::ne-string)


(s/def ::backup
  ::ne-string)


(s/def ::kw-or-string
  (s/or :keyword keyword?
        :string ::ne-string))


(s/def ::attr
  ::kw-or-string)


(s/def ::item
  (s/map-of ::attr any?))


(s/def ::sql-condition
  ::ne-string)


(s/def ::attr-keys
  (s/map-of keyword? keyword?))


(s/def ::attr-vals
  (s/map-of keyword? any?))


(s/def ::consistent-read?
  boolean?)


(s/def ::attrs-get
  (s/coll-of ::kw-or-string))


(s/def ::set
  (s/map-of ::kw-or-string ::kw-or-string))

(s/def ::add
  (s/map-of ::kw-or-string ::kw-or-string))

(s/def ::remove
  (s/coll-of ::kw-or-string))

(s/def ::delete
  (s/map-of ::kw-or-string ::kw-or-string))


(s/def ::scan-forward?
  boolean?)


(s/def ::sql-filter
  ::ne-string)


(s/def ::limit
  integer?)


(s/def ::start-key
  ::item)


(s/def ::sql-key
  ::ne-string)


(s/def ::select
  #{const/select-all-attributes
    const/select-all-projected-attributes
    const/select-specific-attributes
    const/select-count})


(s/def ::return-values
  #{const/return-values-none
    const/return-values-all-old
    const/return-values-updated-old
    const/return-values-all-new
    const/return-values-updated-new})


(s/def ::return-consumed-capacity
  #{const/return-consumed-capacity-indexes
    const/return-consumed-capacity-total
    const/return-consumed-capacity-none})


(s/def ::return-item-collection-metrics
  #{const/return-item-collection-metrics-size
    const/return-item-collection-metrics-none})


(s/def ::callback
  fn?)


;;
;; API
;;

(s/fdef api/delete-table
  :args
  (s/cat :client ::client
         :table ::table
         :options
         (s/?
          (s/nilable
           (s/keys
            :opt-un [::callback])))))


(s/fdef api/describe-table
  :args
  (s/cat :client ::client
         :table ::table
         :options
         (s/?
          (s/nilable
           (s/keys
            :opt-un [::callback])))))


(s/fdef api/put-item
  :args
  (s/cat :client ::client
         :table ::table
         :item ::item
         :options
         (s/?
          (s/nilable
           (s/keys
            :opt-un [::sql-condition
                     ::attr-keys
                     ::attr-vals
                     ::return-consumed-capacity
                     ::return-item-collection-metrics
                     ::return-values
                     ::callback])))))


(s/fdef api/get-item
  :args
  (s/cat :client ::client
         :table ::table
         :key ::item
         :options
         (s/?
          (s/nilable
           (s/keys
            :opt-un [::attrs-get
                     ::attr-keys
                     ::consistent-read?
                     ::return-consumed-capacity
                     ::return-item-collection-metrics
                     ::callback])))))


(s/fdef api/delete-item
  :args
  (s/cat :client ::client
         :table ::table
         :key ::item
         :options
         (s/?
          (s/nilable
           (s/keys
            :opt-un [::sql-condition
                     ::attrs-get
                     ::attr-keys
                     ::attr-vals
                     ::return-consumed-capacity
                     ::return-item-collection-metrics
                     ::return-values
                     ::callback])))))


(s/fdef api/udpate-item
  :args
  (s/cat :client ::client
         :table ::table
         :key ::item
         :options
         (s/?
          (s/nilable
           (s/keys
            :opt-un [::sql-condition
                     ::attr-keys
                     ::attr-vals
                     ::set
                     ::add
                     ::remove
                     ::delete
                     ::return-consumed-capacity
                     ::return-item-collection-metrics
                     ::return-values
                     ::callback])))))


(s/fdef api/query
  :args
  (s/cat :client ::client
         :table ::table
         :options
         (s/?
          (s/nilable
           (s/keys
            :opt-un [::consistent-read?
                     ::sql-filter
                     ::index
                     ::limit
                     ::scan-forward?
                     ::start-key
                     ::select
                     ::return-consumed-capacity
                     ::sql-key
                     ::attrs-get
                     ::attr-keys
                     ::attr-vals
                     ::callback])))))


(s/fdef api/create-backup
  :args
  (s/cat :client ::client
         :table ::table
         :backup ::backup
         :options
         (s/?
          (s/nilable
           (s/keys
            :opt-un [::callback])))))
