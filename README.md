# Relational Database Loader

[![Build Status][build-image]][build]
[![Release][release-image]][releases]
[![License][license-image]][license]
[![Scala Steward][scala-steward-image]][scala-steward]
[![Coverage Status][coveralls-image]][coveralls]

## Introduction

This project contains applications required to load Snowplow data into various data warehouses.

It consists of two types of applications: Transformers and Loaders

### Transformers

Transformers read Snowplow enriched events, transform them to a format ready to be loaded to a data warehouse, then write them to respective blob storage.

There are two types of Transformers: Batch and Streaming

#### Stream Transformer

Stream Transformers read enriched events from respective stream service, transform them, then write transformed events to specified blob storage path.
They write transformed events in periodic windows.

There are two different Stream Transformer applications: Transformer Kinesis and Transformer Pubsub. As one can predict, they are different variants for GCP and AWS.


#### Batch Transformer

It is a [Spark][spark] job. It only works with AWS services. It reads enriched events from a given S3 path, transforms them, then writes transformed events to a specified S3 path.


### Loaders

Transformers send a message to a message queue after they are finished with transforming some batch and writing it to blob storage.
This message contains information about transformed data such as where it is stored and what it looks like.

Loaders subscribe to the message queue. After a message is received, it is parsed, and necessary bits are extracted to load transformed events to the destination.
Loaders construct necessary SQL statements to load transformed events then they send these SQL statements to the specified destination.

At the moment, we have loader applications for Redshift, Databricks and Snowflake.

## Find out more

| Technical Docs             | Setup Guide          | Roadmap & Contributing |
|----------------------------|----------------------|------------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image]   |
| [Technical Docs][techdocs] | [Setup Guide][setup] | [Roadmap][roadmap]     |

## Copyright and License

Snowplow Relational Database Loader is copyright 2012-2022 Snowplow Analytics Ltd.

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
[setup]: https://docs.snowplow.io/docs/getting-started-on-snowplow-open-source/
[techdocs]: https://docs.snowplow.io/docs/pipeline-components-and-applications/loaders-storage-targets/snowplow-rdb-loader/
[roadmap]: https://github.com/snowplow/snowplow/projects/7

[spark]: http://spark.apache.org/

[build-image]: https://github.com/snowplow/snowplow-rdb-loader/workflows/Test%20and%20deploy/badge.svg
[build]: https://github.com/snowplow/snowplow-rdb-loader/actions?query=workflow%3A%22Test%22

[release-image]: https://img.shields.io/badge/release-5.1.0-blue.svg?style=flat
[releases]: https://github.com/snowplow/snowplow-rdb-loader/releases

[license-image]: https://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: https://www.apache.org/licenses/LICENSE-2.0

[scala-steward-image]: https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=
[scala-steward]: https://scala-steward.org

[coveralls]: https://coveralls.io/github/snowplow/snowplow-rdb-loader?branch=master
[coveralls-image]: https://coveralls.io/repos/github/snowplow/snowplow-rdb-loader/badge.svg?branch=master
