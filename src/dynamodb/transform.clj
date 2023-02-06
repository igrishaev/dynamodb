(ns dynamodb.transform
  (:require
   [clojure.string :as str]))


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


(defn build-expr [tag form]
  (when form
    (case tag

      :set
      (str "SET "
           (str/join ", " (for [[k v] form]
                            (format "%s = %s" k v))))

      :remove
      (str "REMOVE "
           (str/join ", " form))

      :add
      (str "ADD "
           (str/join ", " (for [[k v] form]
                            (format "%s %s" k v))))

      :delete
      (str "DELETE "
           (str/join ", " (for [[k v] form]
                            (format "%s %s" k v)))))))


(defn update-expression
  [tag->form]
  (when-let [exprs
             (reduce-kv
              (fn [acc k v]
                (if-let [expr (build-expr k v)]
                  (conj acc expr)
                  acc))
              nil
              tag->form)]
    (str/join " " (reverse exprs))))
