(ns dynamodb.time
  (:import
   java.time.Instant
   java.time.ZoneId
   java.time.format.DateTimeFormatter))


(def ^DateTimeFormatter
  aws-formatter
  (-> "yyyyMMdd'T'HHmmss'Z'"
      (DateTimeFormatter/ofPattern)
      (.withZone (ZoneId/of "UTC"))))


(defn aws-now []
  (.format aws-formatter (Instant/now)))
