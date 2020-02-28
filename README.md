# Relational Database Loader

[![Build Status][travis-image]][travis]
[![Release][release-image]][releases]
[![License][license-image]][license]

## Introduction

This project contains applications required to load Snowplow data into relational databases.

### RDB Shredder

RDB Shredder is a [Spark][spark] job which:

1. Reads Snowplow enriched events from S3
2. Extracts any unstructured event JSONs and context JSONs found
3. Validates that these JSONs conform to schema
4. Adds metadata to these JSONs to track their origins
5. Writes these JSONs out to nested folders dependent on their schema

It is designed to be run by the [EmrEtlRunner][emr-etl-runner] immediately after the [Spark Enrich][spark-enrich] job.

### RDB Loader

RDB Loader (previously known as StorageLoader) is a Scala application that runs as AWS EMR step, discovering [data][shred], produced by RDB Shredder and loading it into one of possible [storage targets][targets].


## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing   |         
|-----------------------------|-----------------------|--------------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image]       |
| [Technical Docs][techdocs] | [Setup Guide][setup] | _coming soon_              |

## Copyright and License

Snowplow Relational Database Loader is copyright 2012-2019 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[setup]: https://github.com/snowplow/snowplow/wiki/6-Configuring-shredding
[techdocs]: https://github.com/snowplow/snowplow/wiki/Relational-Database-Loader

[spark]: http://spark.apache.org/
[emr-etl-runner]: https://github.com/snowplow/snowplow/tree/master/3-enrich/emr-etl-runner
[spark-enrich]: https://github.com/snowplow/snowplow/tree/master/3-enrich/spark-enrich

[targets]: https://github.com/snowplow/snowplow/wiki/Configuring-storage-targets
[shred]: https://github.com/snowplow/snowplow/wiki/Scala-Hadoop-Shred

[travis-image]: https://travis-ci.org/snowplow/snowplow-rdb-loader.png?branch=master
[travis]: http://travis-ci.org/snowplow/snowplow-rdb-loader

[release-image]: http://img.shields.io/badge/release-r32-blue.svg?style=flat
[releases]: https://github.com/snowplow/snowplow-rdb-loader/releases

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0
