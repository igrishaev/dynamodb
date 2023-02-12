(ns dynamodb.sql)


(deftype SQL [^String string]
  Object
  (toString [_]
    string))


(defn sql [string]
  (new SQL string))


(defn sql? [obj]
  (instance? SQL obj))
