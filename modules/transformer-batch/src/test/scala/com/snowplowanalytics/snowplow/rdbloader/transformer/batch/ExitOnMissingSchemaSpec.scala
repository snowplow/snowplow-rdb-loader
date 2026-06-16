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

import java.time.Instant
import java.util.UUID

import org.specs2.mutable.Specification

import org.apache.spark.SparkException

import cats.data.NonEmptyList
import cats.implicits._

import io.circe.Json
import io.circe.parser

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure}
import com.snowplowanalytics.snowplow.badrows.FailureDetails.LoaderIgluError

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.client.ClientError

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.Contexts

import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec._

abstract class ExitOnMissingSchemaSpec extends Specification with ShredJobSpec {
  override def appName = "exit-on-missing-schema"
  sequential

  val UnknownSchema  = SchemaKey("com.acme", "unknown", "jsonschema", SchemaVer.Full(1, 0, 0))
  val unknownContext = SelfDescribingData[Json](UnknownSchema, Json.obj())

  val uuid = UUID.randomUUID()
  val event = Event
    .minimal(
      id              = uuid,
      collectorTstamp = Instant.now(),
      vCollector      = "1.2.3",
      vEtl            = "4.5.6"
    )
    .copy(contexts = Contexts(List(unknownContext)))

  val lines = Lines(
    event.toTsv
  )

  def parseBadRow(s: String): Either[String, BadRow] =
    for {
      json <- parser.parse(s).leftMap(_.message)
      sdj <- json.as[SelfDescribingData[BadRow]].leftMap(_.message)
    } yield sdj.data
}

class ExitOnMissingSchemaSpec_1 extends ExitOnMissingSchemaSpec {
  "A shred job with exitOnMissingIgluSchema set to true" should {
    "throw an exception when a schema is missing" in {
      runShredJob(
        lines,
        tsv                     = true,
        exitOnMissingIgluSchema = true
      ) must throwA[SparkException].like { case e: SparkException =>
        e.getCause.getMessage must contain(
          s"RDB Transformer failed because Iglu schema ${UnknownSchema.toSchemaUri} cannot be fetched"
        )
      }
    }
  }
}

class ExitOnMissingSchemaSpec_2 extends ExitOnMissingSchemaSpec {
  "A shred job with exitOnMissingIgluSchema set to true and a missing schema in skipSchemas" should {
    val criterion = SchemaCriterion(UnknownSchema.vendor, UnknownSchema.name, UnknownSchema.format, UnknownSchema.version.model)
    runShredJob(
      lines,
      tsv                     = true,
      skipSchemas             = List(criterion),
      exitOnMissingIgluSchema = true
    )

    "not write a bad row for the missing schema" in {
      dirs.badRows must beEmptyDir
    }

    "write an atomic event" in {
      val Some((lines, _)) = readPartFile(dirs.goodRows, AtomicFolder)
      lines.size must beEqualTo(1)
      lines.head must beMatching(s".*$uuid.*")
    }

    "not write a file for the missing schema" in {
      val folder = List(
        s"vendor=${UnknownSchema.vendor}",
        s"name=${UnknownSchema.name}",
        "format=tsv",
        s"model=${UnknownSchema.version.model}",
        s"revision=${UnknownSchema.version.revision}",
        s"addition=${UnknownSchema.version.addition}"
      ).mkString("/")

      readPartFile(dirs.goodRows, folder) should beNone
    }
  }
}

class ExitOnMissingSchemaSpec_3 extends ExitOnMissingSchemaSpec {
  "A shred job with exitOnMissingIgluSchema set to false" should {
    runShredJob(
      lines,
      tsv                     = true,
      exitOnMissingIgluSchema = false
    )

    "write a LoaderIgluError bad row for a missing schema" in {
      val Some((jsons, _)) = readPartFile(dirs.badRows, "")
      jsons.size must beEqualTo(1)
      parseBadRow(jsons.head) must beRight.like {
        case BadRow.LoaderIgluError(
              _,
              Failure.LoaderIgluErrors(NonEmptyList(LoaderIgluError.IgluError(UnknownSchema, ClientError.ResolutionError(_)), Nil)),
              _
            ) =>
          ok
        case _ => ko(s"[$jsons] is not one LoaderIgluError bad row with expected ResolutionError")
      }
    }

    "not write a good event for the missing schema" in {
      dirs.goodRows must beEmptyDir
    }
  }
}
