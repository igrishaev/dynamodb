(ns dynamodb.client
  (:import
   java.net.URI)
  (:require
   [dynamodb.util :refer [as]]
   [clojure.java.io :as io]
   [dynamodb.time :as time]
   [cheshire.core :as json]
   [clj-aws-sign.core :as aws-sign]
   ;; [clojure.string :as str]
   [org.httpkit.client :as http]))


(defn make-request
  [{:keys [host
           path
           region
           access-key
           secret-key
           endpoint
           content-type
           version
           service
           async?]}
   target
   data]

  (let [payload
        (json/generate-string data)

        date
        (time/aws-now)

        amz-target
        (format "DynamoDB_%s.%s" version target)

        auth-header
        (aws-sign/authorize
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
          :access-key access-key
          :secret-key secret-key})

        headers
        {"authorization" auth-header
         "content-type" content-type
         "x-amz-target" amz-target
         "x-amz-date" date}]

    (-> {:method :post
         :url endpoint
         :body payload
         :headers headers
         :as :stream}

        (http/request

         (fn [{:keys [status ^InputStream body]}]

           (let [data-parsed
                 (with-open [r (io/reader body :encoding "UTF-8")]
                   (json/parse-stream r keyword))

                 {:keys [__type
                         Message]} data-parsed]

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
                  :message Message
                  :payload data
                  :target target})

               data-parsed))))

        (as [response]
          (if async?
            response
            @response)))))
