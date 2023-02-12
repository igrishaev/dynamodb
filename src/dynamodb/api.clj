(ns dynamodb.api
  (:import
   #_:clj-kondo/ignore java.util.List
   #_:clj-kondo/ignore java.util.Map
   java.net.URI)
  (:require
   [dynamodb.sql :as sql]
   [dynamodb.client :as client]
   [dynamodb.constant :as const]
   [dynamodb.mask :as mask]
   [dynamodb.params :refer [pre-process]]
   [dynamodb.response :refer [post-process]]))


(defn sql [string]
  (sql/sql string))


(defn make-client

  ([access-key secret-key endpoint region]
   (make-client access-key secret-key endpoint region nil))

  ([access-key secret-key endpoint region
    {:keys [throw?
            version]
     :or {throw? false
          version const/version-20120810}}]

   (let [uri
         (new URI endpoint)

         host
         (.getHost uri)

         path
         (.getPath uri)]

     {:access-key (mask/mask access-key)
      :secret-key (mask/mask secret-key)
      :endpoint endpoint
      :content-type "application/x-amz-json-1.0"
      :host host
      :path path
      :service "dynamodb"
      :version version
      :region region
      :throw? throw?})))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html
;; https://cloud.yandex.com/en-ru/docs/ydb/docapi/api-ref/actions/createTable
(defn create-table

  {:arglists
   '([client]
     [client
      {:keys [^Map    tags
              ^String table-class
              ^String billing-mode
              ^List   provisioned-throughput
              ^Map    global-indexes
              ^Map    local-indexes
              ^Map    stream-spec
              ^Map    sse-spec]}])}

  ([client table attr-defs key-schema]
   (create-table client table attr-defs key-schema nil))

  ([client table attr-defs key-schema options]

   (-> options
       (assoc :table table
              :attr-defs attr-defs
              :key-schema key-schema)
       (pre-process)
       (->> (client/make-request client "CreateTable"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateBackup.html
(defn create-backup

  ([client table backup]
   (create-backup client table backup nil))

  ([client table backup options]

   (-> options
       (assoc :table table :backup backup)
       (pre-process)
       (->> (client/make-request client "CreateBackup"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeBackup.html
(defn describe-backup

  ([client backup-arn]
   (describe-backup client backup-arn nil))

  ([client backup-arn options]

   (-> options
       (assoc :backup-arn backup-arn)
       (pre-process)
       (->> (client/make-request client "DescribeBackup"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html
(defn delete-table
  ([client table]
   (delete-table client table nil))

  ([client table options]

   (-> options
       (assoc :table table)
       (pre-process)
       (->> (client/make-request client "DeleteTable"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ListTables.html
(defn list-tables

  {:arglists
   '([client]
     [client
      {:keys [^Long limit
              ^String start-table]}])}

  ([client]
   (list-tables client nil))

  ([client options]

   (-> options
       (pre-process)
       (->> (client/make-request client "ListTables"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeTable.html
(defn describe-table

  ([client table]
   (describe-table client table nil))

  ([client table options]
   (-> options
       (assoc :table table)
       (pre-process)
       (->> (client/make-request client "DescribeTable"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html
(defn put-item

  {:arglists
   '([client table item]
     [client table item
      {:keys [^String sql-condition
              ^Map    attr-names
              ^Map    attr-values
              ^String return-consumed-capacity
              ^String return-item-collection-metrics
              ^String return-values]}])}

  ([client table item]
   (put-item client table item nil))

  ([client table item options]

   (-> options
       (assoc :table table :item item)
       (pre-process)
       (->> (client/make-request client "PutItem"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html
(defn get-item

  {:arglists
   '([client table key]
     [client table key {:keys [^List    attrs-get
                               ^Map     attr-names
                               ^Boolean consistent-read?
                               ^String  return-consumed-capacity]}])}

  ([client table key]
   (get-item client table key nil))

  ([client table key options]

   (-> options
       (assoc :table table :key key)
       (pre-process)
       (->> (client/make-request client "GetItem"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
(defn batch-get-item

  {:arglists
   '([client table->options]
     [client table->options
      {:keys [^String return-consumed-capacity]}])}

  ([client table->options]
   (batch-get-item client table->options nil))

  ([client table->options options]

   (-> options
       (assoc :request-items table->options)
       (pre-process)
       (->> (client/make-request client "BatchGetItem"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html
(defn delete-item

  {:arglists
   '([client table key]
     [client table key
      {:keys [^String sql-condition
              ^Map    attr-names
              ^Map    attr-values
              ^String return-consumed-capacity
              ^String return-item-collection-metrics
              ^String return-values]}])}

  ([client table key]
   (delete-item client table key nil))

  ([client table key options]

   (-> options
       (assoc :table table :key key)
       (pre-process)
       (->> (client/make-request client "DeleteItem"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
;; https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html
(defn update-item

  {:arglists
   '([client table key]
     [client table key
      {:keys [^String sql-condition
              ^Map    attr-names
              ^Map    attr-values
              ^Map    set
              ^Map    add
              ^List   remove
              ^Map    delete
              ^String return-consumed-capacity
              ^String return-item-collection-metrics
              ^String return-values]}])}

  ([client table key]
   (update-item client table key nil))

  ([client table key options]

   (-> options
       (assoc :table table :key key)
       (pre-process)
       (->> (client/make-request client "UpdateItem"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html
;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
(defn query

  {:arglists
   '([client table]
     [client table
      {:keys [^Boolean consistent-read?
              ^String  sql-filter
              ^String  index
              ^Long    limit
              ^Boolean scan-forward?
              ^Map     start-key
              ^String  select
              ^Boolean return-consumed-capacity
              ^String  sql-key
              ^List    attrs-get
              ^Map     attr-names
              ^Map     attr-values]}])}

  ([client table]
   (query client table nil))

  ([client table options]

   (-> options
       (assoc :table table)
       (pre-process)
       (->> (client/make-request client "Query"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html
(defn scan

  {:arglists
   '([client table]
     [client table
      {:keys [^Map     attr-names
              ^Map     attr-values
              ^List    attrs-get
              ^Boolean consistent-read?
              ^String  sql-filter
              ^Boolean index
              ^Long    limit
              ^String  return-consumed-capacity
              ^Long    segment
              ^String  select
              ^Map     start-key
              ^Long    total-segments]}])}

  ([client table]
   (scan client table nil))

  ([client table options]

   (-> options
       (assoc :table table)
       (pre-process)
       (->> (client/make-request client "Scan"))
       (post-process))))


;; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TagResource.html
(defn tag-resource

  ([client resource-arn tags]
   (tag-resource client resource-arn tags nil))

  ([client resource-arn tags options]

   (-> options
       (assoc :tags tags :resource-arn resource-arn)
       (pre-process)
       (->> (client/make-request client "TagResource"))
       (post-process))))
