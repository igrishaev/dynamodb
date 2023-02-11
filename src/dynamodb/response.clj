(ns dynamodb.response
  (:require
   [dynamodb.util :as util]
   [dynamodb.decode :refer [decode-attrs]]))


(defn- -decode-items [items]
  (mapv decode-attrs items))


(defn post-process [response]
  (when-not (= response {})
    (cond-> response

      (:Item response)
      (update :Item decode-attrs)

      (:Attributes response)
      (update :Attributes decode-attrs)

      (:Items response)
      (update :Items -decode-items)

      (:Responses response)
      (update :Responses util/update-vals -decode-items)

      (:UnprocessedKeys response)
      (update :UnprocessedKeys
              util/update-vals
              (fn [entry]
                (update entry :Keys -decode-items)))

      (:LastEvaluatedKey response)
      (update :LastEvaluatedKey decode-attrs))))
