# DynamoDB

[dynamodb]: https://aws.amazon.com/dynamodb/

A [DynamoDB][dynamodb] driver written in pure Clojure. Lightweight
dependencies. GraalVM/native-image friendly.

## Table of Contents

<!-- toc -->

- [Benefits](#benefits)
- [Installation](#installation)
- [API Implemented](#api-implemented)
- [Usage](#usage)
- [Raw API access](#raw-api-access)
- [Tests](#tests)

<!-- tocstop -->

## Benefits

[http-kit]: https://github.com/http-kit/http-kit
[cheshire]: https://github.com/dakrone/cheshire

[native-image]: https://www.graalvm.org/22.0/reference-manual/native-image/

- Free from AWS SDK. Everything is implemented with pure JSON + HTTP.
- Quite narrow dependencies: just [HTTP Kit][http-kit] and [Cheshire][cheshire].
- Compatible with [Native Image][native-image]! Thus, easy to use as a binary
  file in AWS Lambda.
- Both encoding & decoding are extendable with protocols & multimethods.
- Raw API access for rare cases.

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
missing in that table.

<details open>
<summary>See the table</summary>

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

## Usage

## Raw API access

## Tests

Ivan Grishaev, 2023
