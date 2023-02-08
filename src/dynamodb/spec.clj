(ns dynamodb.spec
  (:require
   [dynamodb.mask :as mask]
   [dynamodb.api :as api]
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
        :string :ne-string))


(s/def ::item
  (s/map-of ::attr any?))


(s/def ::sql-condition ::ne-string)


(s/def ::attr-keys
  (s/map-of keyword? keyword?))


(s/def ::attr-vals
  (s/map-of keyword? any?))


;;
;; API
;;

(s/fdef api/delete-table
  :args
  (s/cat :client ::client
         :table ::table))


(s/fdef api/put-table
  :args
  (s/cat :client ::client
         :table ::table
         :item ::item
         :params
         (s/nilable
          (s/keys
           :opt-un [::sql-condition
                    ::attr-keys
                    ::attr-vals
                    ::return-consumed-capacity
                    ::return-item-collection-metrics
                    ::return-values]))))
