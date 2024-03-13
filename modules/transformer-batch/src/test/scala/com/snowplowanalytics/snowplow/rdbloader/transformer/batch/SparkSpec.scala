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

// Spark
import org.apache.spark.sql.SparkSession

/**
 * Trait to mix in in every spec which has to run a Spark job. Create a spark session before the
 * spec and delete it afterwards.
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
    SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

  override def beforeAll(): Unit = {
    val _ = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    spark = null
  }
}
