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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.good.widerow

import java.util.UUID

import org.apache.spark.sql.types._

import io.circe.{Json, JsonObject}
import io.circe.syntax._

import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Formats.WideRow
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.Contexts
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.Main
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.MessageTypeParser
import org.specs2.mutable.Specification

import java.io.{File, FileFilter}
import java.time.temporal.ChronoUnit

import scala.jdk.CollectionConverters._

class WideRowParquetSpec extends Specification with ShredJobSpec {
  override def appName = "wide-row"
  sequential
  "A job which is configured for wide row parquet output" should {
    val testOutputDirs = OutputDirs(randomFile("output"))
    val inputEvents = readResourceFile(ResourceFile("/widerow/parquet/input-events"))
    runShredJob(
      events = ResourceFile("/widerow/parquet/input-events"),
      wideRow = Some(WideRow.PARQUET),
      outputDirs = Some(testOutputDirs)
    )

    "transform the enriched event to wide row parquet" in {
      val badEventIds = List(
        UUID.fromString("3ebc0e5e-340e-414b-b67d-23f7948c2df2"),
        UUID.fromString("7f2c98b2-4a3f-49c0-806d-e8ea2f580ef7")
      )
      val lines = readParquetFile(spark, testOutputDirs.goodRows)
        .sortBy(_.asObject.flatMap(_("event_id")).flatMap(_.asString))

      val expectedLines = inputEvents
        .flatMap(Event.parse(_).toOption)
        .filter(e => !badEventIds.contains(e.event_id))
        .sortBy(_.event_id.toString)
        .map(transformEventForParquetTest("none"))

      assertGeneratedParquetSchema(testOutputDirs)
      lines.size must beEqualTo(46)

      forall(lines.zip(expectedLines)) { case (line, expectedLine) =>
        line must beEqualTo(expectedLine)
      }
    }

    "write bad rows" in {
      val Some((lines, _)) = readPartFile(testOutputDirs.badRows)
      val expected = readResourceFile(ResourceFile("/widerow/parquet/output-badrows"))
        .map(_.replace(VersionPlaceholder, BuildInfo.version))
      lines.size must beEqualTo(4)
      lines.toSet mustEqual(expected.toSet)
    }

    "have SparkConf with outputTimestampType property is set to TIMESTAMP_MICROS" in {
      Main.sparkConfig.get("spark.sql.parquet.outputTimestampType") must beEqualTo("TIMESTAMP_MICROS")
    }
  }

  // This test case uses events which contains contexts with
  // different versions of test schema.
  "A job which is configured for wide row parquet output" should {
    val testOutputDirs = OutputDirs(randomFile("output"))
    val inputEvents = readResourceFile(ResourceFile("/widerow/parquet/input-events-custom-contexts"))
    runShredJob(
      events = ResourceFile("/widerow/parquet/input-events-custom-contexts"),
      wideRow = Some(WideRow.PARQUET),
      outputDirs = Some(testOutputDirs)
    )

    "transform the enriched event with test contexts to wide row parquet" in {
      val lines = readParquetFile(spark, testOutputDirs.goodRows)
        .sortBy(_.asObject.flatMap(_("event_id")).flatMap(_.asString))
      val expectedLines = inputEvents
        .flatMap(Event.parse(_).toOption)
        .sortBy(_.event_id.toString)
        .map(transformEventForParquetTest("contexts_com_snowplowanalytics_snowplow_parquet_test_a_1"))

      lines.size must beEqualTo(100)
      forall(lines.zip(expectedLines)) { case (line, expectedLine) =>
        line must beEqualTo(expectedLine)
      }
    }

    "set parquet column types correctly" in {
      val customPart = readParquetFields(spark, testOutputDirs.goodRows)
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
      customPart.find("i_field").find("c_field").dataType must beEqualTo(LongType)
      customPart.find("i_field").find("c_field").nullable must beTrue
      customPart.find("j_field").find("union").dataType must beEqualTo(StringType)
      customPart.find("j_field").find("union").nullable must beTrue
    }
  }

  // This test case uses events which contains unstruct events with
  // different versions of test schema.
  "A job which is configured for wide row parquet output" should {
    val testOutputDirs = OutputDirs(randomFile("output"))
    val inputEvents = readResourceFile(ResourceFile("/widerow/parquet/input-events-custom-unstruct"))
    runShredJob(
      events = ResourceFile("/widerow/parquet/input-events-custom-unstruct"),
      wideRow = Some(WideRow.PARQUET),
      outputDirs = Some(testOutputDirs)
    )

    "transform the enriched event with test unstruct events to wide row parquet" in {
      val lines = readParquetFile(spark, testOutputDirs.goodRows)
        .toSet
      val expected = inputEvents
        .flatMap(Event.parse(_).toOption)
        .map(transformEventForParquetTest("unstruct_event_com_snowplowanalytics_snowplow_parquet_test_a_1"))
        .toSet
      lines.size must beEqualTo(100)
      lines must beEqualTo(expected)
    }

    "set parquet column types correctly" in {
      val customPart = readParquetFields(spark, testOutputDirs.goodRows)
        .find("unstruct_event_com_snowplowanalytics_snowplow_parquet_test_a_1")
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
      customPart.find("i_field").find("c_field").dataType must beEqualTo(LongType)
      customPart.find("i_field").find("c_field").nullable must beTrue
      customPart.find("j_field").find("union").dataType must beEqualTo(StringType)
      customPart.find("j_field").find("union").nullable must beTrue
    }
  }

  def transformEventForParquetTest(entityColumnName: String)(e: Event): Json = {
    val json = e.copy(
      // Due to a bug in the Scala Analytics SDK's toJson method, derived_contexts overrides contexts with same schemas.
      // In order to circumvent this problem, contexts and derived_contexts are combined under context
      // and derived_contexts is made empty list.
      contexts = Contexts(e.contexts.data ::: e.derived_contexts.data),
      derived_contexts = Contexts(List.empty),
      // Since parquet is using java.sql.Timestamp instead of Instant and
      // Timestamp's precision is less than Instant's precision, we are truncating
      // event's timestamps to match them to parquet output.
      collector_tstamp = e.collector_tstamp.truncatedTo(ChronoUnit.MILLIS),
      derived_tstamp = e.derived_tstamp.map(_.truncatedTo(ChronoUnit.MILLIS)),
      etl_tstamp = e.etl_tstamp.map(_.truncatedTo(ChronoUnit.MILLIS)),
      dvce_created_tstamp = e.dvce_created_tstamp.map(_.truncatedTo(ChronoUnit.MILLIS)),
      dvce_sent_tstamp = e.dvce_sent_tstamp.map(_.truncatedTo(ChronoUnit.MILLIS)),
      refr_dvce_tstamp = e.refr_dvce_tstamp.map(_.truncatedTo(ChronoUnit.MILLIS)),
      true_tstamp = e.true_tstamp.map(_.truncatedTo(ChronoUnit.MILLIS))
    ).toJson(true).deepDropNullValues

    def normalizeKeys(j: Json): Json =
      j.arrayOrObject(
        j,
        vec => Json.fromValues(vec.map(normalizeKeys)),
        obj => 
          JsonObject.fromIterable(obj.toList.map {
            case (k, v) => k.replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase -> normalizeKeys(v)
          }).asJson
      )

    val jsonTransformer: Json => Json = { j: Json =>
      j.hcursor
        // The following fields are not part of schemas therefore
        // they shouldn't be in parquet formatted data.
        .withFocus(j => j.asObject.map(_.remove("k_field").remove("l_field").remove("m_field").asJson).getOrElse(j))
        // The following fields are union types in the original event, e.g. {integer or boolean}.
        // In parquet format they are serialized as a JSON string, i.e. `"42"` instead of `42` and `"false"` instead of `false`.
        // Therefore we serialize them to a JSON string in this json too.
        .withFocus(j => j.hcursor.downField("e_field").withFocus(f => if (f.isNull) f else f.noSpaces.asJson).top.getOrElse(j))
        .withFocus(j => j.hcursor.downField("f_field").withFocus(f => if (f.isNull) f else f.noSpaces.asJson).top.getOrElse(j))
        .withFocus(j => j.hcursor.downField("j_field").downField("union").withFocus(f => if (f.isNull) f else f.noSpaces.asJson).top.getOrElse(j))
        .top.getOrElse(j)
    }

    val transformed = json.hcursor
      .downField(entityColumnName)
      .withFocus(_.arrayOrObject[Json](
        "".asJson,
        _.map(jsonTransformer).asJson,
        j => jsonTransformer(j.asJson)
      ))
      .top.getOrElse(json)

    normalizeKeys(transformed)
  }

  def assertGeneratedParquetSchema(testOutputDirs: OutputDirs) = {
    val conf = new Configuration();
    val expectedParquetSchema = readResourceFile(ResourceFile("/widerow/parquet/parquet-output-schema")).mkString
    val expectedColumns = MessageTypeParser.parseMessageType(expectedParquetSchema).getColumns.asScala

    val parquetFileFilter = new FileFilter {
      override def accept(pathname: File): Boolean = pathname.toString.endsWith(".parquet")
    }

    testOutputDirs.goodRows.listFiles(parquetFileFilter).forall { parquetFile =>
      val parquetMetadata = ParquetFileReader.readFooter(conf, new Path(parquetFile.toString), ParquetMetadataConverter.NO_FILTER)
      val columns = parquetMetadata.getFileMetaData.getSchema.getColumns.asScala

      foreach(columns.zip(expectedColumns)) { case (col, expectedCol) =>
        col.toString mustEqual expectedCol.toString
      }
    }
  }

  implicit class ParquetFields(val fields: List[StructField]) {
    def find(fieldName: String): StructField =
      fields.find(_.name == fieldName).get
  }

  implicit class ParquetArrayType(val field: StructField) {
    def find(fieldName: String): StructField =
      field.dataType match {
        case ArrayType(StructType(fields), _) => fields.toList.find(fieldName)
        case StructType(fields) => fields.toList.find(fieldName)
        case _ => null
      }
  }
}
