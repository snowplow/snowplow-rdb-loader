/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import io.sentry.{Sentry, SentryOptions}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import cats.syntax.either._

import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark.Serialization

object Main {

  lazy val sparkConfig: SparkConf = new SparkConf()
    .setAppName(getClass.getSimpleName)
    .setIfMissing("spark.master", "local[*]")
    .set("spark.serializer", classOf[KryoSerializer].getName)
    .set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    .registerKryoClasses(Serialization.classesToRegister)

  def main(args: Array[String]): Unit =
    CliConfig.loadConfigFrom(BuildInfo.name, BuildInfo.description)(args) match {
      case Right(cli) =>
        val spark = SparkSession
          .builder()
          .config(sparkConfig)
          .getOrCreate()
        val sentryClient = cli.config.monitoring.sentry.map(s => Sentry.init(SentryOptions.defaults(s.dsn.toString)))
        Either
          .catchNonFatal(ShredJob.run(spark, cli.igluConfig, cli.duplicateStorageConfig, cli.config)) match {
          case Left(throwable) =>
            sentryClient.foreach(_.sendException(throwable))
            spark.stop()
            throw throwable
          case Right(_) =>
            spark.stop()
        }
      case Left(error) =>
        System.err.println(error)
        System.exit(2)
    }
}
