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


(defn key->proj-expr
  [k]
  (cond

    (keyword? k)
    (-> k str (subs 1))

    (string? k)
    k

    :else
    (throw (ex-info "Wrong projection placeholder"
                    {:key k}))))
