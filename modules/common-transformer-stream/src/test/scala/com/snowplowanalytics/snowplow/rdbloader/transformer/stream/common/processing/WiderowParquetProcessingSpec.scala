/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import fs2.io.file.Path

import com.github.mjakubowski84.parquet4s.{Path => ParquetPath, RowParquetRecord}
import com.github.mjakubowski84.parquet4s.parquet.fromParquet
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.Contexts
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.AppId
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.ParquetUtils
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.ParquetUtils.readParquetColumns
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing.BaseProcessingSpec.TransformerConfig
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing.WiderowParquetProcessingSpec.{appConfig, igluConfig}
import io.circe.syntax.EncoderOps
import io.circe.{Json, JsonObject}
import org.apache.parquet.column.ColumnDescriptor

import java.io.File
import java.time.temporal.ChronoUnit
import java.util.UUID

class WiderowParquetProcessingSpec extends BaseProcessingSpec {

  val badEventIds = List(
    UUID.fromString("3ebc0e5e-340e-414b-b67d-23f7948c2df2"),
    UUID.fromString("7f2c98b2-4a3f-49c0-806d-e8ea2f580ef7")
  )

  "Streaming transformer" should {
    "process items correctly in widerow parquet format" in {
      temporaryDirectory
        .use { outputDirectory =>
          val inputStream = InputEventsProvider.eventStream(
            inputEventsPath =
              "/processing-spec/4/input/events" // the same events as in resource file used in WideRowParquetSpec for batch transformer
          )

          val config = TransformerConfig(appConfig(outputDirectory), igluConfig)
          val goodPath = Path(outputDirectory.toString + s"/run=1970-01-01-10-30-00-${AppId.appId}/output=good")
          val badPath = Path(outputDirectory.toString + s"/run=1970-01-01-10-30-00-${AppId.appId}/output=bad")

          for {
            output <- process(inputStream, config)
            expectedParquetColumns <- readParquetColumnsFromResource(
                                        "/processing-spec/4/output/good/parquet/schema"
                                      ) // the same schema as in resource file used in WideRowParquetSpec for batch transformer
            actualParquetRows <- readParquetRowsFrom(goodPath, expectedParquetColumns)
            actualParquetColumns = readParquetColumns(goodPath)
            actualBadRows <- readStringRowsFrom(badPath)

            expectedCompletionMessage <- readMessageFromResource("/processing-spec/4/output/good/parquet/completion.json", outputDirectory)
            expectedBadRows <- readLinesFromResource("/processing-spec/4/output/badrows")
            expectedParquetRows <- readGoodParquetEventsFromResource("/processing-spec/4/input/events", columnToAdjust = None)
          } yield {

            actualParquetRows.size must beEqualTo(46)
            actualBadRows.size must beEqualTo(4)
            removeAppId(output.completionMessages.toList) must beEqualTo(List(expectedCompletionMessage))
            output.checkpointed must beEqualTo(1)

            assertParquetRows(actualParquetRows, expectedParquetRows)
            assertParquetColumnsInAllFiles(actualParquetColumns, expectedParquetColumns)
            assertStringRows(actualBadRows, expectedBadRows)
          }
        }
        .unsafeRunSync()
    }

    "process items with custom contexts correctly in widerow parquet format" in {
      temporaryDirectory
        .use { outputDirectory =>
          val inputStream = InputEventsProvider.eventStream(
            inputEventsPath =
              "/processing-spec/5/input/input-events-custom-contexts" // the same events as in resource file used in WideRowParquetSpec for batch transformer
          )

          val config = TransformerConfig(appConfig(outputDirectory), igluConfig)
          val goodPath = Path(outputDirectory.toString + s"/run=1970-01-01-10-30-00-${AppId.appId}/output=good")

          for {
            output <- process(inputStream, config)
            expectedParquetColumns <- readParquetColumnsFromResource("/processing-spec/5/output/good/parquet/schema")
            actualParquetRows <- readParquetRowsFrom(goodPath, expectedParquetColumns)
            actualParquetColumns = readParquetColumns(goodPath)
            expectedCompletionMessage <- readMessageFromResource("/processing-spec/5/output/good/parquet/completion.json", outputDirectory)
            expectedParquetRows <- readGoodParquetEventsFromResource(
                                     "/processing-spec/5/input/input-events-custom-contexts",
                                     columnToAdjust = Some("contexts_com_snowplowanalytics_snowplow_parquet_test_a_1")
                                   )
          } yield {

            actualParquetRows.size must beEqualTo(100)
            removeAppId(output.completionMessages.toList) must beEqualTo(List(expectedCompletionMessage))
            output.checkpointed must beEqualTo(1)

            assertParquetRows(actualParquetRows, expectedParquetRows)
            assertParquetColumnsInAllFiles(actualParquetColumns, expectedParquetColumns)
          }
        }
        .unsafeRunSync()
    }
    "process items with custom unstruct correctly in widerow parquet format" in {
      temporaryDirectory
        .use { outputDirectory =>
          val inputStream = InputEventsProvider.eventStream(
            inputEventsPath =
              "/processing-spec/6/input/input-events-custom-unstruct" // the same events as in resource file used in WideRowParquetSpec for batch transformer
          )

          val config = TransformerConfig(appConfig(outputDirectory), igluConfig)
          val goodPath = Path(outputDirectory.toString + s"/run=1970-01-01-10-30-00-${AppId.appId}/output=good")

          for {
            output <- process(inputStream, config)
            expectedParquetColumns <- readParquetColumnsFromResource("/processing-spec/6/output/good/parquet/schema")
            actualParquetRows <- readParquetRowsFrom(goodPath, expectedParquetColumns)
            actualParquetColumns = readParquetColumns(goodPath)
            expectedCompletionMessage <- readMessageFromResource("/processing-spec/6/output/good/parquet/completion.json", outputDirectory)
            expectedParquetRows <-
              readGoodParquetEventsFromResource(
                "/processing-spec/6/input/input-events-custom-unstruct",
                columnToAdjust = Some("unstruct_event_com_snowplowanalytics_snowplow_parquet_test_a_1")
              )
          } yield {

            actualParquetRows.size must beEqualTo(100)
            removeAppId(output.completionMessages.toList) must beEqualTo(List(expectedCompletionMessage))
            output.checkpointed must beEqualTo(1)

            assertParquetRows(actualParquetRows, expectedParquetRows)
            assertParquetColumnsInAllFiles(actualParquetColumns, expectedParquetColumns)
          }
        }
        .unsafeRunSync()
    }
    "Recover from the broken schema migrations" in {
      temporaryDirectory
        .use { outputDirectory =>
          val inputStream = InputEventsProvider.eventStream(
            inputEventsPath =
              "/processing-spec/7/input/events" // the same events as in resource file used in WideRowParquetSpec for batch transformer
          )

          val config = TransformerConfig(appConfig(outputDirectory), igluConfig)
          val goodPath = Path(outputDirectory.toString + s"/run=1970-01-01-10-30-00-${AppId.appId}/output=good")

          for {
            output <- process(inputStream, config)
            expectedParquetColumns <- readParquetColumnsFromResource(
                                        "/processing-spec/7/output/good/parquet/schema"
                                      ) // the same schema as in resource file used in WideRowParquetSpec for batch transformer
            actualParquetRows <- readParquetRowsFrom(goodPath, expectedParquetColumns)
            actualParquetColumns = readParquetColumns(goodPath)
            expectedCompletionMessage <- readMessageFromResource("/processing-spec/7/output/good/parquet/completion.json", outputDirectory)
          } yield {

            actualParquetRows.size must beEqualTo(3)
            removeAppId(output.completionMessages.toList) must beEqualTo(List(expectedCompletionMessage))
            output.checkpointed must beEqualTo(1)

            forall(
              actualParquetRows
                .map(_.toString())
                .zip(
                  List(
                    """(?s).*contexts_com_snowplowanalytics_snowplow_test_schema_broken_1\"\s*:\s*\[\s*\{\s*\"b_field\"\s*:\s*1\s*\}\s*\].*""".r,
                    """(?s).*contexts_com_snowplowanalytics_snowplow_test_schema_broken_1\"\s*:\s*\[\s*\{\s*\"b_field\"\s*:\s*2\s*\}\s*\].*""".r,
                    """(?s).*contexts_com_snowplowanalytics_snowplow_test_schema_broken_1_recovered_1_0_1_1837344102\"\s*:\s*\[\s*\{\s*\"b_field\"\s*:\s*\"s\"\s*\}\s*\].*""".r
                  )
                )
            ) { case (actual, expected) =>
              actual must beMatching(expected)
            }
            assertParquetColumnsInAllFiles(actualParquetColumns, expectedParquetColumns)
          }
        }
        .unsafeRunSync()
    }
  }

  private def readGoodParquetEventsFromResource(resource: String, columnToAdjust: Option[String]) =
    readLinesFromResource(resource)
      .map { events =>
        events
          .flatMap(Event.parse(_).toOption)
          .filter(e => !badEventIds.contains(e.event_id))
          .sortBy(_.event_id.toString)
          .map(transformEventForParquetTest(columnToAdjust.getOrElse("none")))
      }

  private def readParquetRowsFrom(path: Path, columns: List[ColumnDescriptor]) =
    fromParquet[IO]
      .as[RowParquetRecord]
      .read(ParquetPath(path.toNioPath.toUri.toString))
      .map { record =>
        ParquetUtils.convertParquetRecordToJson(record, List.empty, columns)
      }
      .compile
      .toList
      .map(_.sortBy(_.asObject.flatMap(_("event_id")).flatMap(_.asString)))
      .map(_.map(_.deepDropNullValues))

  private def readParquetColumnsFromResource(path: String): IO[List[ColumnDescriptor]] =
    readLinesFromResource(path)
      .map(_.mkString)
      .map(ParquetUtils.extractColumnsFromSchemaString)

  private def assertParquetRows(actualRows: List[Json], expectedRows: List[Json]) =
    forall(actualRows.zip(expectedRows)) { case (actual, expected) =>
      actual must beEqualTo(expected)
    }

  private def assertParquetColumnsInAllFiles(
    actualColumnsPerFile: Map[File, List[ColumnDescriptor]],
    expectedColumns: List[ColumnDescriptor]
  ) =
    foreach(actualColumnsPerFile.values) { columns =>
      foreach(columns.zip(expectedColumns)) { case (actual, expected) =>
        actual.toString mustEqual expected.toString
      }
    }

  // copied from WideRowParquetSpec in batch transformer
  private def transformEventForParquetTest(entityColumnName: String)(e: Event): Json = {
    val json = e
      .copy(
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
      )
      .toJson(true)
      .deepDropNullValues

    def normalizeKeys(j: Json): Json =
      j.arrayOrObject(
        j,
        vec => Json.fromValues(vec.map(normalizeKeys)),
        obj =>
          JsonObject
            .fromIterable(obj.toList.map { case (k, v) =>
              k.replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase -> normalizeKeys(v)
            })
            .asJson
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
        .withFocus(j =>
          j.hcursor.downField("j_field").downField("union").withFocus(f => if (f.isNull) f else f.noSpaces.asJson).top.getOrElse(j)
        )
        .top
        .getOrElse(j)
    }

    val transformed = json.hcursor
      .downField(entityColumnName)
      .withFocus(
        _.arrayOrObject[Json](
          "".asJson,
          _.map(jsonTransformer).asJson,
          j => jsonTransformer(j.asJson)
        )
      )
      .top
      .getOrElse(json)

    normalizeKeys(transformed)
  }
}

object WiderowParquetProcessingSpec {
  private val appConfig = (outputPath: Path) => s"""|{
        | "input": {
        |    "type": "kinesis"
        |    "streamName": "test-stream"
        |    "region": "eu-central-1"
        |    "appName": "snowplow-transformer"
        |    "position": "LATEST"
        |    "retrievalMode": {
        |      "type": "Polling"
        |      "maxRecords": 10000
        |    }
        |    "bufferSize": 3
        | }
        | "output": {
        |   "path": "${outputPath.toNioPath.toUri.toString}"
        |   "compression": "NONE"
        |   "region": "eu-central-1"
        | }
        | "queue": {
        |   "type": "SQS"
        |   "queueName": "notUsed"
        |   "region": "eu-central-1"
        | }
        | "windowing": "1 minute"
        | "formats": {
        |   "transformationType": "widerow"
        |   "fileFormat": "parquet"
        | }
        |}""".stripMargin

  private val igluConfig =
    """|{
       |  "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-2",
       |  "data": {
       |    "cacheSize": 5000,
       |    "cacheTtl": 10000,
       |    "repositories": [
       |      {
       |        "name": "Iglu Test Embedded",
       |        "priority": 0,
       |        "vendorPrefixes": [ ],
       |        "connection": {
       |          "embedded": {
       |            "path": "/"
       |          }
       |        }
       |      }
       |    ]
       |  }
       |}""".stripMargin

}
