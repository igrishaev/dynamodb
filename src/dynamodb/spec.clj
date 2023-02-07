(ns dynamodb.spec
  (:require
   [dynamodb.api :as api]
   [clojure.string :as str]
   [clojure.spec.alpha :as s]))


(s/def ::ne-string
  (s/and string? (complement str/blank?)))


(s/def ::access-key ::ne-string)
(s/def ::secret-key ::ne-string)
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
           :opt-un [::condition
                    ::attr-names
                    ::attr-values
                    ::return-consumed-capacity
                    ::return-item-collection-metrics
                    ::return-values]))))
