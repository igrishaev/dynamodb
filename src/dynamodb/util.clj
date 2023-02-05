(ns dynamodb.util
  (:refer-clojure :exclude [update-vals]))


(defn update-vals [m f]
  (persistent!
   (reduce-kv
    (fn [acc! k v]
      (assoc! acc! k (f v)))
    (transient {})
    m)))


(defmacro as
  {:style/indent 1}
  [x [bind] & body]
  `(let [~bind ~x]
     ~@body))
