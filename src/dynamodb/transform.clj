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


(defn keyword->name-placeholder [k]
  (cond
    (keyword? k)
    (format "#%s" (name k))
    (string? k)
    k
    :else
    (throw (ex-info "Wrong attribute placeholder"
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
           (->> form
                (map keyword->name-placeholder)
                (str/join ", ")))

      :add
      (str "ADD "
           (str/join ", " (for [[k v] form]
                            (format "%s %s" k v))))

      :delete
      (str "DELETE "
           (str/join ", "
                     (for [[k v] form]
                       (format "%s %s" (keyword->name-placeholder k) v)))))))


(defn add-expr [form]
  (when form
    (str "ADD "
         (str/join ", " (for [[k v] form]
                          (format "%s %s" k v))))))


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


(defn encode-attr-names [mapping]
  (reduce-kv
   (fn [acc k v]
     (assoc acc (format "#%s" (name k)) v))
   {}
   mapping))
