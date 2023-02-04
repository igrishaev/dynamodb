(ns dynamodb.util)


(defn update-vals [m f]
  (persistent!
   (reduce-kv
    (fn [acc! k v]
      (assoc! acc! k (f v)))
    (transient {})
    m)))
