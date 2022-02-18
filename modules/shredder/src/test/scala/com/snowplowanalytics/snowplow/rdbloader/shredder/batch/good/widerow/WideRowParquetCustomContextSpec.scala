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
package com.snowplowanalytics.snowplow.rdbloader.shredder.batch.good.widerow

import org.apache.spark.sql.types._

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Formats.WideRow
import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.ShredJobSpec
import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.ShredJobSpec._
import org.specs2.mutable.Specification

class WideRowParquetCustomContextSpec extends Specification with ShredJobSpec {
  override def appName = "wide-row"
  sequential
  "A job which is configured for wide row parquet output" should {
    val inputEvents = readResourceFile(ResourceFile("/widerow/parquet/input-events-custom-contexts"))
    runShredJob(events = ResourceFile("/widerow/parquet/input-events-custom-contexts"), wideRow = Some(WideRow.Parquet))

    "transform the enriched event to wide row parquet" in {
      val lines = readParquetFile(spark, dirs.goodRows)
        .toSet
      val expected = inputEvents
        .flatMap(Event.parse(_).toOption)
        .map(testEventUpdate)
        .map(_.toJson(true).deepDropNullValues)
        .toSet
      lines.size must beEqualTo(100)
      lines must beEqualTo(expected)
    }

    "set parquet column types correctly" in {
      val customPart = readParquetFields(spark, dirs.goodRows)
        .find("contexts_com_snowplowanalytics_snowplow_parquet_test_a_1")
      customPart.find("e_field").dataType must beEqualTo(StringType)
      customPart.find("e_field").nullable must beTrue
      customPart.find("f_field").dataType must beEqualTo(StringType)
      customPart.find("f_field").nullable must beTrue
      customPart.find("g_field").dataType must beEqualTo(StringType)
      customPart.find("g_field").nullable must beTrue
      customPart.find("h_field").dataType must beEqualTo(TimestampType)
      customPart.find("h_field").nullable must beTrue
      customPart.find("i_field").find("b_field").dataType must beEqualTo(StringType)
      customPart.find("i_field").find("b_field").nullable must beTrue
      customPart.find("i_field").find("c_field").dataType must beEqualTo(IntegerType)
      customPart.find("i_field").find("c_field").nullable must beTrue
      customPart.find("j_field").find("union").dataType must beEqualTo(StringType)
      customPart.find("j_field").find("union").nullable must beTrue
    }
  }
}
