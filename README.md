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
- [Raw API access](#raw-api-access)
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

At the moment, only the most important API targets are implemented. The rest of
them is a matter of time and copy-paste. Let me know if you need something
missing in the table below.

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

DynamoDB is a part of [Teleward][teleward] -- a Telegram captcha bot. The bot is
hosted in Yandex Cloud as a binary file compiled with GraalVM. It uses the
library to reach Yandex DB to track the state. In turn, Yandex DB is a cloud
database that mimics DynamoDB and serves a subset of its HTTP API.

## Usage

First, import the library. You need only the `.api` module:

```clojure
(require '[dynamodb.api :as api])
```

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

The fifth parameter is a map of options to override:

| Parameter   | Default      | Description                                                               |
|-------------|--------------|---------------------------------------------------------------------------|
| `:throw?`   | `true`       | Whether to throw an exception when get a negative response from DynamoDB. |
| `:version`  | `"20120810"` | DynamoDB API version.                                                     |
| `:http-opt` | (see below)  | A map for HTTP Kit default settings.                                      |

The default HTTP settings are:

```clojure
{:user-agent "com.github.igrishaev/dynamodb"
 :keepalive (* 30 1000)
 :insecure? true
 :follow-redirects false}
```

### Create a Table

### List Tables

### Put Item

### Get Item

### Update Item

### Delete Item

### Query

### Scan

## Raw API access

The `api-call` function allows you to interact with DynamoDB on low level. It
accepts the client, the target name, and a raw payload you'd like send to
DB. The payload gets sent as-is with no any kind of processing nor interference.

```clojure
(api/api-call CLIENT
             "NotImplementedTarget"
             {:ParamFoo ... :ParamBar ...})
```

## Tests

The primary testing module called `api_test.clj` relies on a local DynamoDB
instance running in Docker. To bootstrap it, execute the command:

```bash
make docker-up
```

which spawns `amazon/dynamodb-local` image on port 8000. Then connect to the REPL
and run the API tests from Emacs like you usually do.

Ivan Grishaev, 2023
