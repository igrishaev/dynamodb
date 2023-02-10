(ns dynamodb.spec
  (:require
   [clojure.spec.alpha :as s]
   [clojure.spec.test.alpha :as spec.test]
   [clojure.string :as str]
   [dynamodb.api :as api]
   [dynamodb.constant :as const]
   [dynamodb.mask :as mask]))


(s/def ::ne-string
  (s/and string? (complement str/blank?)))


;;
;; Enum
;;

(s/def ::attr-type
  #{"S" "N" "B" :S :N :B})


(s/def ::key-type
  #{const/key-type-hash
    const/key-type-range})


(s/def ::sse-type
  #{const/sse-type-aes256
    const/sse-type-kms})


(s/def ::stream-view-type
  #{const/stream-view-type-new-image
    const/stream-view-type-old-image
    const/stream-view-type-new-and-old-images
    const/stream-view-type-keys-only})

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


(s/def ::table-class
  #{const/table-class-standard
    const/table-class-standard-infrequent_access})


(s/def ::billing-mode
  #{const/billing-mode-provisioned
    const/billing-mode-pay-per-request})


(s/def ::projection-type
  #{const/projection-type-all
    const/projection-type-keys-only
    const/projection-type-include})



;;
;; Fields
;;

(s/def ::access-key mask/masked?)
(s/def ::secret-key mask/masked?)
(s/def ::endpoint   ::ne-string)
(s/def ::region     ::ne-string)


(s/def ::attr-defs
  (s/map-of ::kw-or-string ::attr-type))


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


(s/def ::backup-arn
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


(s/def ::resource-arn
  ::ne-string)


(s/def ::key-schema
  (s/map-of ::kw-or-string ::key-type))


(s/def ::tags
  (s/map-of ::kw-or-string ::kw-or-string))


(s/def ::provisioned-throughput
  (s/tuple number? number?))


(s/def ::non-key-attrs
  (s/coll-of ::kw-or-string))


(s/def ::projection
  (s/keys :req-un [::non-key-attrs
                   ::type]))


(s/def ::indexes
  (s/map-of ::kw-or-string
            (s/keys :req-un [::key-schema
                             ::projection]
                    :opt-un [::provisioned-throughput])))


(s/def ::global-indexes
  ::indexes)


(s/def ::local-indexes
  ::indexes)


(s/def ::enabled?
  boolean?)


(s/def ::stream-spec
  (s/keys :req-un [::enabled?
                   ::type]))


(s/def ::kms-key-id
  ::ne-string)


(s/def ::sse-spec
  (s/keys :opt-un [::enabled?
                   ::kms-key-id
                   ::type]))


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
           (s/map-of keyword? any?)))))


(s/fdef api/describe-table
  :args
  (s/cat :client ::client
         :table ::table
         :options
         (s/?
          (s/nilable
           (s/map-of keyword? any?)))))


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
                     ::return-values])))))


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
                     ::return-item-collection-metrics])))))


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
                     ::return-values])))))


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
                     ::return-values])))))


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
                     ::attr-vals])))))


(s/fdef api/create-backup
  :args
  (s/cat :client ::client
         :table ::table
         :backup ::backup
         :options
         (s/?
          (s/nilable
           (s/map-of keyword? any?)))))


(s/fdef api/describe-backup
  :args
  (s/cat :client ::client
         :backup-arn ::backup-arn
         :options
         (s/?
          (s/nilable
           (s/map-of keyword? any?)))))


(s/fdef api/tag-resource
  :args
  (s/cat :client ::client
         :resource-arn ::resource-arn
         :tags ::tags
         :options
         (s/?
          (s/nilable
           (s/map-of keyword? any?)))))


(s/fdef api/create-table
  :args
  (s/cat :client ::client
         :table ::table
         :attr-defs ::attr-defs
         :key-schema ::key-schema
         :options
         (s/?
          (s/nilable
           (s/keys
            :opt-un [::tags
                     ::table-class
                     ::billing-mode
                     ::provisioned-throughput
                     ::global-indexes
                     ::local-indexes
                     ::stream-spec
                     ::sse-spec])))))
