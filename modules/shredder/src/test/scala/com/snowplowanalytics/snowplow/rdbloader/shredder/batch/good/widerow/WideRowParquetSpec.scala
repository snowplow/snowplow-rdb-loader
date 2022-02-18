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

import java.util.UUID

import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Formats.WideRow
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.ShredJobSpec
import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.ShredJobSpec._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import org.specs2.mutable.Specification

class WideRowParquetSpec extends Specification with ShredJobSpec {
  override def appName = "wide-row"
  sequential
  "A job which is configured for wide row parquet output" should {
    val inputEvents = readResourceFile(ResourceFile("/widerow/parquet/input-events"))
    runShredJob(events = ResourceFile("/widerow/parquet/input-events"), wideRow = Some(WideRow.Parquet))

    "transform the enriched event to wide row parquet" in {
      val badEventIds = List(
        UUID.fromString("3ebc0e5e-340e-414b-b67d-23f7948c2df2"),
        UUID.fromString("7f2c98b2-4a3f-49c0-806d-e8ea2f580ef7")
      )
      val lines = readParquetFile(spark, dirs.goodRows).toSet
      val expected = inputEvents
        .flatMap(Event.parse(_).toOption)
        .map(testEventUpdate)
        .filter(e => !badEventIds.contains(e.event_id))
        .map(_.toJson(true).deepDropNullValues)
        .toSet
      lines.size must beEqualTo(46)
      lines must beEqualTo(expected)
    }

    "write bad rows" in {
      val Some((lines, _)) = readPartFile(dirs.badRows)
      val expected = readResourceFile(ResourceFile("/widerow/parquet/output-badrows"))
        .map(_.replace(VersionPlaceholder, BuildInfo.version))
      lines.size must beEqualTo(4)
      lines.toSet mustEqual(expected.toSet)
    }
  }
}
