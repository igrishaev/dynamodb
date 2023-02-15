# DynamoDB

[dynamodb]: https://aws.amazon.com/dynamodb/

A [DynamoDB][dynamodb] driver written in pure Clojure. Lightweight
dependencies. GraalVM/native-image friendly.

## Table of Contents

<!-- toc -->

- [Benefits](#benefits)
- [Installation](#installation)
- [API Implemented](#api-implemented)
- [Who Uses It](#who-uses-it)
- [Usage](#usage)
  * [The Client](#the-client)
  * [Create a Table](#create-a-table)
  * [List Tables](#list-tables)
  * [Put Item](#put-item)
  * [Get Item](#get-item)
  * [Update Item](#update-item)
  * [Delete Item](#delete-item)
  * [Query](#query)
  * [Scan](#scan)
  * [Other API](#other-api)
- [Raw API access](#raw-api-access)
- [Specs](#specs)
- [Tests](#tests)

<!-- tocstop -->

## Benefits

[http-kit]: https://github.com/http-kit/http-kit
[cheshire]: https://github.com/dakrone/cheshire

[native-image]: https://www.graalvm.org/22.0/reference-manual/native-image/
[ydb]: https://cloud.yandex.com/en-ru/services/ydb

- Free from AWS SDK. Everything is implemented with pure JSON + HTTP.
- Quite narrow dependencies: just [HTTP Kit][http-kit] and [Cheshire][cheshire].
- Compatible with [Native Image][native-image]! Thus, easy to use as a binary
  file in AWS Lambda.
- Clojure-friendly: handles attributes with namespaces and builds proper SQL
  expressions.
- Both encoding & decoding are extendable with protocols & multimethods.
- Raw API access for rare cases.
- Specs for better input validation.
- Compatible with [Yandex DB][ydb].

## Installation

Leiningen/Boot:

```
[com.github.igrishaev/dynamodb "0.1.2"]
```

Clojure CLI/deps.edn:

```
com.github.igrishaev/dynamodb {:mvn/version "0.1.2"}
```

## API Implemented

[api-ops]: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Operations_Amazon_DynamoDB.html

At the moment, only the most important [API targets][api-ops] are
implemented. The rest of them is a matter of time and copy-paste. Let me know if
you need something missing in the table below.

<details>
<summary>Check out the table</summary>

| Target                              | Done? | Comment |
|-------------------------------------|-------|---------|
| BatchExecuteStatement               |       |         |
| BatchGetItem                        | +     |         |
| BatchWriteItem                      |       |         |
| CreateBackup                        | +     |         |
| CreateGlobalTable                   |       |         |
| CreateTable                         | +     |         |
| DeleteBackup                        |       |         |
| DeleteItem                          | +     |         |
| DeleteTable                         | +     |         |
| DescribeBackup                      | +     |         |
| DescribeContinuousBackups           |       |         |
| DescribeContributorInsights         |       |         |
| DescribeEndpoints                   |       |         |
| DescribeExport                      |       |         |
| DescribeGlobalTable                 |       |         |
| DescribeGlobalTableSettings         |       |         |
| DescribeImport                      |       |         |
| DescribeKinesisStreamingDestination |       |         |
| DescribeLimits                      |       |         |
| DescribeTable                       | +     |         |
| DescribeTableReplicaAutoScaling     |       |         |
| DescribeTimeToLive                  |       |         |
| DisableKinesisStreamingDestination  |       |         |
| EnableKinesisStreamingDestination   |       |         |
| ExecuteStatement                    |       |         |
| ExecuteTransaction                  |       |         |
| ExportTableToPointInTime            |       |         |
| GetItem                             | +     |         |
| ImportTable                         |       |         |
| ListBackups                         |       |         |
| ListContributorInsights             |       |         |
| ListExports                         |       |         |
| ListGlobalTables                    |       |         |
| ListImports                         |       |         |
| ListTables                          | +     |         |
| ListTagsOfResource                  |       |         |
| PutItem                             | +     |         |
| Query                               | +     |         |
| RestoreTableFromBackup              |       |         |
| RestoreTableToPointInTime           |       |         |
| Scan                                | +     |         |
| TagResource                         | +     |         |
| TransactGetItems                    |       |         |
| TransactWriteItems                  |       |         |
| UntagResource                       |       |         |
| UpdateContinuousBackups             |       |         |
| UpdateContributorInsights           |       |         |
| UpdateGlobalTable                   |       |         |
| UpdateGlobalTableSettings           |       |         |
| UpdateItem                          | +     |         |
| UpdateTable                         |       |         |
| UpdateTableReplicaAutoScaling       |       |         |
| UpdateTimeToLive                    |       |         |

</details>

## Who Uses It

[teleward]: https://github.com/igrishaev/teleward

DynamoDB is a part of [Teleward][teleward] — a Telegram captcha bot. The bot is
hosted in Yandex Cloud as a binary file compiled with GraalVM. It uses the
library to track the state in Yandex DB. In turn, Yandex DB is a cloud database
that mimics DynamoDB and serves a subset of its HTTP API.

## Usage

First, import the library:

```clojure
(require '[dynamodb.api :as api])
(require '[dynamodb.constant :as const])
```

The `constant` module is needed sometimes to refer to common DynamoDB values
like `"PAY_PER_REQUEST"`, `"PROVISIONED"` and so on.

### The Client

Prepare a client object. The first four parameters are mandatory:

```clojure
(def CLIENT
  (api/make-client "aws-public-key"
                   "aws-secret-key"
                   "https://aws.dynamodb.endpoint.com/some/path"
                   "aws-region"
                   {...}))
```

For Yandex DB, the region is something like "ru-central1".

Both public and secret AWS keys are masked with a special wrapper that prevents
them from being logged or printed.

The fifth parameter is a map of options to override:

| Parameter   | Default      | Description                                    |
|-------------|--------------|------------------------------------------------|
| `:throw?`   | `true`       | Whether to throw a negative DynamoDB response. |
| `:version`  | `"20120810"` | DynamoDB API version.                          |
| `:http-opt` | (see below)  | A map of HTTP Kit default settings.            |

The default HTTP settings are:

```clojure
{:user-agent "com.github.igrishaev/dynamodb"
 :keepalive (* 30 1000)
 :insecure? true
 :follow-redirects false}
```

### Create a Table

To create a new table, pass its name, the schema map, and the primary key
mapping:

```clojure
(api/create-table CLIENT
                  "SomeTable"
                  {:user/id :N
                   :user/name :S}
                  {:user/id const/key-type-hash
                   :user/name const/key-type-range}
                  {:tags {:foo "hello"}
                   :table-class const/table-class-standard
                   :billing-mode const/billing-mode-pay-per-request})
```

### List Tables

Tables can be listed by pages. The default page size 100. Once you're reached
the limit, check out the `LastEvaluatedTableName` field. Pass it to the
`:start-table` optional argument to propagate to the next page:

```clojure
(def resp1
  (api/list-tables CLIENT {:limit 10}))

(def last-table
  (:LastEvaluatedTableName resp1))

(def resp2
  (api/list-tables CLIENT
                   {:limit 10
                    :start-table last-table}))
```

### Put Item

To upsert an item, pass a map that contains the primary attributes:

```clojure
(api/put-item CLIENT
              "SomeTable"
              {:user/id 1
               :user/name "Ivan"
               :user/foo 1}
              {:return-values const/return-values-none})
```

Pass `:sql-condition` to make the operation conditional. In the example above,
the `:user/foo` attribute is 1. The second upsert operation checks if
`:user/foo` is either 1, or 2, or 3, which is true. Thus, it will fail:

```clojure
(api/put-item CLIENT
              "SomeTable"
              {:user/id 1
               :user/name "Ivan"
               :user/test 3}
              {:sql-condition "#foo in (:one, :two, :three)"
               :attr-names {"#foo" :user/foo}
               :attr-values {":one" 1
                             ":two" 2
                             ":three" 3}
               :return-values const/return-values-all-old})
```

### Get Item

To get an item, provide its primary key:

```clojure
(api/get-item CLIENT
              "SomeTable"
              {:user/id 1
               :user/name "Ivan"})

{:Item #:user{:id 1
              :name "Ivan"
              :foo 1}}
```

There is an option to get only the attributes you need or even sub-attributes
for nested maps or lists:

```clojure
;; put some complex values
(api/put-item CLIENT
              "SomeTable"
              {:user/id 1
               :user/name "Ivan"
               :test/kek "123"
               :test/foo 1
               :abc nil
               :foo "lol"
               :bar {:baz [1 2 3]}})

;; pass a list of attributes/paths into the `:attrs-get` param
(api/get-item CLIENT
              "SomeTable"
              {:user/id 1
               :user/name "Ivan"}
              {:attrs-get [:test/kek "bar.baz[1]" "abc" "foo"]})

;; the result:
{:Item {:test/kek "123"
        :bar {:baz [2]}
        :abc nil
        :foo "lol"}}
```

### Update Item

[faraday]: https://github.com/Taoensso/faraday

This operation is the most complex. In AWS SDK or [Faraday][faraday], to update
secondary attributes of an item, one should build a SQL expression manually
which involves string formatting, concatenation and similar boring stuff.

```sql
SET username = :username, email = :email, ...
```

The `ADD`, `DELETE`, and `REMOVE` expression require manual work as well.

The present library solves this problem for you. The `update-item` function
accepts `:add`, `:set`, `:delete`, and `:remove` parameters which are either
maps or vectors.

#### Set Attributes

```clojure
(api/update-item CLIENT
                 table
                 {:user/id 1
                  :user/name "Ivan"}
                 {:attr-names {"#counter" :test/counter}
                  :attr-values {":one" 1}
                  :set {"Foobar" 123
                        :user/email "test@test.com"
                        "#counter" (api/sql "#counter + :one")}})
```

The example above covers three various options for the `:set` argument. Namely:

1. The attribute is a plain string `("Foobar")`, and the value is plain as well.
2. The attribute is a complex keyword (`:user/email`) which cannot be placed in
   a SQL expression directrly. Unser the hood, the library produces an alias for
   it.
3. The


### Delete Item

Simple deletion of an item:

```clojure
(api/delete-item CLIENT
                 table
                 {:user/id 1 :user/name "Ivan"})
```

Conditional deletion: throws an exception when the expression fails.

```clojure
(api/put-item CLIENT
              table
              {:user/id 1
               :user/name "Ivan"
               :test/kek 99})

(api/delete-item CLIENT
                 table
                 {:user/id 1 :user/name "Ivan"}
                 {:sql-condition "#kek in (:foo, :bar, :baz)"
                  :attr-names {"#kek" :test/kek}
                  :attr-values {":foo" 1
                                ":bar" 2
                                ":baz" 3}})
```

In the example above, the `"#kek in (:foo, :bar, :baz)"` expression fails as the
`:test/kek` attribute is of value 99. The item stays in the database, and you'll
get an exception with ex-info:

```clojure
{:error? true
 :status 400
 :path "com.amazonaws.dynamodb.v20120810"
 :exception "ConditionalCheckFailedException"
 :message "The conditional request failed"
 :payload
 {:TableName table
  :Key #:user{:id {:N "1"} :name {:S "Ivan"}}
  :ConditionExpression "#kek in (:foo, :bar, :baz)"
  :ExpressionAttributeNames {"#kek" :test/kek}
  :ExpressionAttributeValues
  {":foo" {:N "1"} ":bar" {:N "2"} ":baz" {:N "3"}}}
 :target "DeleteItem"}
```

### Query

The Query target allows to seach items that partially match a primary
key. Imagine the primary key of a table is `:user/id :HASH` and `:user/name
:RANGE`. Here is what you have in the database:

```clojure
{:user/id 1
 :user/name "Ivan"
 :test/foo 1}

{:user/id 1
 :user/name "Juan"
 :test/foo 2}

{:user/id 2
 :user/name "Huan"
 :test/foo 3}
```

Now, to find the items whose `:user/id` is 1, execute:

```clojure
(api/query CLIENT
           table
           {:sql-key "#id = :one"
            :attr-names {"#id" :user/id}
            :attr-values {":one" 1}
            :limit 1})
```

Result:

```
{:Items [{:user/id 1
          :test/foo 1
          :user/name "Ivan"}]
 :Count 1
 :ScannedCount 1
 :LastEvaluatedKey #:user{:id 1
                          :name "Ivan"}}
```

To propagate to the next page, fetch the `LastEvaluatedKey` field from the
result and pass it into the `:start-key` Query parameter.


### Scan

The Scan API goes through the whole table collecting the items that match an
expression. This is unoptimal yet required sometimes.

```clojure
(api/scan CLIENT
          table
          {:sql-filter "#foo = :two"
           :attrs-get [:test/foo "#name"]
           :attr-names {"#foo" :test/foo
                        "#name" :user/name}
           :attr-values {":two" 2}
           :limit 2})
```

Result:

```
{:Items [{:test/foo 2
          :user/name "Ivan"}]
 :Count 1
 :ScannedCount 2
 :LastEvaluatedKey #:user{:id 1
                          :name "Ivan"}}
```

Both `LastEvaluatedKey` and `:start-key` parameters work as described above.

### Other API

See the tests, specs, and `dynamodb.api` module for more information.

## Raw API access

The `api-call` function allows you to interact with DynamoDB on low level. It
accepts the client, the target name, and a raw payload you'd like send to
DB. The payload gets sent as-is with no any kind of processing nor interference.

```clojure
(api/api-call CLIENT
             "NotImplementedTarget"
             {:ParamFoo ... :ParamBar ...})
```

## Specs

The library provides a number of specs for the API. Find them in the
`dynamodb.spec` module. It's not imported by default to prevent the binary file
growing when compiled with GraalVM. That's a known issue when introducing
`clojure.spec` adds +20 Mbs to the file.

Still, those specs are usefull for testing and documentation. Import the specs,
then instrument the functions by calling the `instrument` function:

```clojure
(require 'dynamodb.spec)
(require '[clojure.spec.test.alpha :as spec.test])

(spec.test/instrument)
```

Now if you pass something wrong into one of the library functions, you'll get a
spec exception.

## Tests

The primary testing module called `api_test.clj` relies on a local DynamoDB
instance running in Docker. To bootstrap it, execute the command:

```bash
make docker-up
```

It spawns `amazon/dynamodb-local` image on port 8000. Now connect to the REPL
and run the API tests from your editor as usual.

Ivan Grishaev, 2023
