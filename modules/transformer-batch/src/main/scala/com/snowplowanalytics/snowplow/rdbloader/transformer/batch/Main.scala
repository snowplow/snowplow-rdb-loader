/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
