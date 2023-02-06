(ns dynamodb.transform
  )


(defn key->attr-placeholder
  [k]
  (cond
    (keyword? k)
    (str k)
    (string? k)
    k
    :else
    (throw (ex-info "Wrong attribute placeholder"
                    {:key k}))))
