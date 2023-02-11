(defproject dynamodb "0.1.0-SNAPSHOT"

  :description
  "DynamoDB in pure Clojure. GraalVM-friendly"

  :url
  "https://github.com/igrishaev/dynamodb"

  :license
  {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
   :url "https://www.eclipse.org/legal/epl-2.0/"}

  :plugins
  [[lein-cljfmt "0.9.2"]]

  :dependencies
  [[http-kit "2.6.0"]
   [cheshire "5.10.0"]
   [clj-aws-sign "0.1.1"]]

  :cljfmt
  {:remove-consecutive-blank-lines? false
   :paths ["src" "test"]}

  :profiles
  {:dev
   {:dependencies
    [[org.clojure/clojure "1.11.1"]]}})
