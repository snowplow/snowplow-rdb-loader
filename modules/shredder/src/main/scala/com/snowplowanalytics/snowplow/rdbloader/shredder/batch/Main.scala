/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.shredder.batch

import io.sentry.{Sentry, SentryOptions}

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

import cats.syntax.either._

import com.snowplowanalytics.snowplow.rdbloader.common.config.{Config, ShredderCliConfig}
import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.spark.Serialization
import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.generated.BuildInfo

object Main {

  lazy val sparkConfig: SparkConf = new SparkConf()
    .setAppName(getClass.getSimpleName)
    .setIfMissing("spark.master", "local[*]")
    .set("spark.serializer", classOf[KryoSerializer].getName)
    .registerKryoClasses(Serialization.classesToRegister)

  def main(args: Array[String]): Unit = {
    ShredderCliConfig.loadConfigFrom(BuildInfo.name, BuildInfo.description)(args) match {
      case Right(cli) =>
        cli.config.shredder match {
          case b: Config.Shredder.Batch =>
            val spark = SparkSession.builder()
              .config(sparkConfig)
              .getOrCreate()
            val sentryClient = cli.config.monitoring.sentry.map(s => Sentry.init(SentryOptions.defaults(s.dsn.toString)))
            Either
              .catchNonFatal(ShredJob.run(spark, cli.igluConfig, cli.duplicateStorageConfig, cli.config, b))
              .leftMap(throwable => sentryClient.fold(())(_.sendException(throwable)))
            spark.stop()
          case other =>
            System.err.println(s"Shredder configuration $other is not for Spark")
            System.exit(2)
        }
      case Left(error) =>
        System.err.println(error)
        System.exit(2)
    }
  }
}
