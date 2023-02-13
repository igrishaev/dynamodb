(ns dynamodb.transform
  (:refer-clojure :exclude [update-vals
                            update-keys]))


(defn update-vals [m f]
  (persistent!
   (reduce-kv
    (fn [acc! k v]
      (assoc! acc! k (f v)))
    (transient {})
    m)))


(defn update-keys [m f]
  (persistent!
   (reduce-kv
    (fn [acc! k v]
      (assoc! acc! (f k) v))
    (transient {})
    m)))
