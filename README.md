# Relational Database Loader

[![Build Status][build-image]][build]
[![Release][release-image]][releases]
[![License][license-image]][license]

## Introduction

This project contains applications required to load Snowplow data into various data warehouses.

It consists of two types of applications: Transformers and Loaders

### Transformers

Transformers read Snowplow enriched events, transform them to a format ready to be loaded to a data warehouse, then write them to respective blob storage.

There are two types of Transformers: Batch and Streaming

#### Stream Transformer

Stream Transformers read enriched events from respective stream service, transform them, then write transformed events to specified blob storage path.
They write transformed events in periodic windows.

There are two different Stream Transformer applications: Transformer Kinesis and Transformer Pubsub. As one can predict, they are different variants for GCP, AWS and Azure.


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

Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.

Licensed under the [Snowplow Community License](https://docs.snowplow.io/community-license-1.0). _(If you are uncertain how it applies to your use case, check our answers to [frequently asked questions](https://docs.snowplow.io/docs/contributing/community-license-faq/).)_


[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[setup]: https://docs.snowplow.io/docs/getting-started-on-snowplow-open-source/
[techdocs]: https://docs.snowplow.io/docs/pipeline-components-and-applications/loaders-storage-targets/snowplow-rdb-loader/
[roadmap]: https://github.com/snowplow/snowplow/projects/7

[spark]: http://spark.apache.org/

[build-image]: https://github.com/snowplow/snowplow-rdb-loader/workflows/CI/badge.svg
[build]: https://github.com/snowplow/snowplow-rdb-loader/actions/workflows/ci.yml

[release-image]: https://img.shields.io/badge/release-5.7.5-blue.svg?style=flat
[releases]: https://github.com/snowplow/snowplow-rdb-loader/releases

[license]: https://docs.snowplow.io/docs/contributing/community-license-faq/
[license-image]: https://img.shields.io/badge/license-Snowplow--Community-blue.svg?style=flat
