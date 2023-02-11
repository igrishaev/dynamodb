(ns dynamodb.encode
  "
  https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html
  "
  (:require
   [dynamodb.util :as util]
   [dynamodb.codec :as codec]))


(defprotocol IEncode
  (-encode [this]))


(defn encode-bytes [^bytes bytea]
  (-> bytea
      (codec/b64-encode)
      (codec/bytes->str "utf-8")))


(extend-protocol IEncode

  (Class/forName "[B")
  (-encode [^bytes this]
    {:B (encode-bytes this)})

  clojure.lang.Keyword
  (-encode [this]
    (-> this str (subs 1) -encode))

  Object
  (-encode [this]
    (-encode (.toString this)))

  Boolean
  (-encode [this]
    {:BOOL this})

  String
  (-encode [this]
    {:S this})

  Number
  (-encode [this]
    {:N this})

  nil
  (-encode [_this]
    {:NULL true})

  clojure.lang.IPersistentVector
  (-encode [this]
    {:L (mapv -encode this)})

  clojure.lang.IPersistentList
  (-encode [this]
    {:L (mapv -encode this)})

  clojure.lang.IPersistentSet
  (-encode [this]
    (let [item (first this)]
      (cond
        (number? item)
        {:NS this}

        (string? item)
        {:SS this}

        (bytes? item)
        {:BS (mapv encode-bytes this)}

        :else
        (throw
         (ex-info
          "Cannot encode a set: either it's empty or its item is of an improper type."
          {:set this
           :item item})))))

  clojure.lang.IPersistentMap
  (-encode [this]
    {:M (util/update-vals this -encode)}))


(defn encode [value]
  (-encode value))


(defn encode-attrs [mapping]
  (util/update-vals mapping -encode))
