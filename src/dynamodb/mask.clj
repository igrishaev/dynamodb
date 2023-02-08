(ns dynamodb.mask
  (:import
   java.io.Writer))


(defprotocol IMask
  (mask [this])
  (unmask [this]))


(deftype Mask [value]

  Object

  (toString [this]
    "<< masked >>")

  IMask

  (mask [this]
    this)

  (unmask [this]
    value))


(defn masked? [object]
  (instance? Mask object))


(extend-protocol IMask

  Object

  (mask [this]
    (new Mask this))

  (unmask [this]
    this))


(defmethod print-method Mask
  [masked ^Writer writer]
  (.write writer (str masked)))
