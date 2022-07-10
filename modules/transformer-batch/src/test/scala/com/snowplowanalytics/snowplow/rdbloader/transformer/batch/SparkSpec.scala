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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

// Spark
import org.apache.spark.sql.SparkSession

/**
 * Trait to mix in in every spec which has to run a Spark job.
 * Create a spark session before the spec and delete it afterwards.
 */
trait SparkSpec extends BeforeAfterAll {
  def appName: String

  val conf = Main.sparkConfig
    .set("spark.kryo.registrationRequired", "true")
    // TimeZone settings is added since we are reading parquet
    // data in tests and we want timestamps to be in UTC format.
    // We don't need to set it in production version since parquet file
    // isn't read in there.
    .set("spark.sql.session.timeZone", "UTC")

  var spark: SparkSession =
    SparkSession.builder()
      .config(conf)
      .getOrCreate()

  override def beforeAll(): Unit = {
    val _ = SparkSession.builder()
      .config(conf)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    spark = null
  }
}
