(ns dynamodb.client
  (:require
   [cheshire.core :as json]
   [clojure.java.io :as io]
   [dynamodb.sign :as sign]
   [dynamodb.time :as time]
   [mask.core :as mask]
   [org.httpkit.client :as http])
  (:import
   java.io.InputStream
   java.net.URI
   java.util.Map
   mask.core.Mask))


(def defaults
  {:user-agent "com.github.igrishaev/dynamodb"
   :keepalive (* 30 1000)
   :insecure? true
   :follow-redirects false})


(defrecord Client
           [^Mask    access-key
            ^Mask    secret-key
            ^String  endpoint
            ^String  content-type
            ^String  host
            ^String  path
            ^String  service
            ^String  version
            ^String  region
            ^Boolean throw?
            ^Map     http-opt])


(defn make-request
  [{:keys [host
           path
           region
           access-key
           secret-key
           content-type
           version
           service
           throw?
           http-opt]}
   target
   data]

  (let [payload
        (json/generate-string data)

        date
        (time/aws-now)

        amz-target
        (format "DynamoDB_%s.%s" version target)

        auth-header
        (sign/authorize
         {:method "POST"
          :uri path
          :date date
          :headers {"host" host
                    "content-type" content-type
                    "x-amz-date" date
                    "x-amz-target" amz-target}
          :payload payload
          :service service
          :region region
          :access-key (mask/unmask access-key)
          :secret-key (mask/unmask secret-key)})

        headers
        {"authorization" auth-header
         "content-type" content-type
         "x-amz-target" amz-target
         "x-amz-date" date}]

    (-> http-opt
        (assoc :body payload
               :headers headers)

        (http/request

         (fn [{:keys [status ^InputStream body]}]

           (-> (with-open [r (io/reader body :encoding "UTF-8")]
                 (json/parse-stream r keyword))

               (as-> data-parsed
                     (let [{:keys [__type
                                   message ;; yandexdb
                                   Message]}
                           data-parsed]

                       (if __type

                         (let [uri
                               (new URI __type)

                               exception
                               (.getFragment uri)

                               path
                               (.getPath uri)]

                           {:error? true
                            :status status
                            :path path
                            :exception exception
                            :message (or Message message)
                            :payload data
                            :target target})

                         data-parsed)))

               (as-> response
                     (if (and (get response :error?) throw?)
                       (throw (ex-info "DynamoDB failure" response))
                       response)))))

        (as-> response
              (let [{:as response :keys [error]}
                    @response]
                (if error
                  (throw error)
                  response))))))
