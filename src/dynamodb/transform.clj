(ns dynamodb.transform
  (:refer-clojure :exclude [update-vals
                            update-keys])
  (:require
   [clojure.string :as str]))


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


(defn add-expr [form]
  (when form
    (str "ADD "
         (str/join ", " (for [[k v] form]
                          (format "%s %s" k v))))))


(defn set-expr [form]
  (when form
    (str "SET "
         (str/join ", " (for [[k v] form]
                          (format "%s = %s" k v))))))


(defn delete-expr [form]
  (when form
    (str "DELETE "
         (str/join ", "
                   (for [[k v] form]
                     (format "%s %s" (keyword->name-placeholder k) v))))))


(defn remove-expr [form]
  (when form
    (str "REMOVE "
         (->> form
              (map keyword->name-placeholder)
              (str/join ", ")))))


(defn build-expr [tag form]
  (case tag

    :set
    (set-expr form)

    :remove
    (remove-expr form)

    :add
    (add-expr form)

    :delete
    (delete-expr form)))


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
