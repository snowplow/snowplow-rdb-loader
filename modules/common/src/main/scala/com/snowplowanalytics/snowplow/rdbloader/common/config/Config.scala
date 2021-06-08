/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.common.config

import java.net.URI
import java.time.Instant
import java.util.UUID

import scala.concurrent.duration.Duration

import cats.syntax.either._
import cats.syntax.show._

import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._

import com.typesafe.config.ConfigFactory
import pureconfig._
import pureconfig.module.circe._
import pureconfig.error.{CannotParse, ConfigReaderFailures}
import monocle.Lens
import monocle.macros.GenLens
import com.snowplowanalytics.iglu.core.SchemaCriterion

import com.snowplowanalytics.snowplow.rdbloader.common._
import com.snowplowanalytics.snowplow.rdbloader.common.config.Config._
import com.snowplowanalytics.snowplow.rdbloader.common.{S3, LoaderMessage}

/**
 * Main config file parsed from HOCON
 * @tparam D kind of supported warehouse
 */
final case class Config[+D <: StorageTarget](name: String,
                                             id: UUID,
                                             region: String,
                                             jsonpaths: Option[S3.Folder],
                                             monitoring: Monitoring,
                                             messageQueue: String,
                                             shredder: Shredder,
                                             storage: D,
                                             formats: Formats,
                                             steps: Set[Step])

object Config {

  val MetricsDefaultPrefix = "snowplow.rdbloader"

  def fromString(s: String): Either[String, Config[StorageTarget]] =
    Either
      .catchNonFatal(ConfigSource.fromConfig(ConfigFactory.parseString(s)))
      .leftMap(error => ConfigReaderFailures(CannotParse(s"Not valid HOCON. ${error.getMessage}", None)))
      .flatMap { config =>
        config
          .load[Json]
          .flatMap { json =>
            json.as[Config[StorageTarget]].leftMap(failure => ConfigReaderFailures(CannotParse(failure.show, None)))
          }
      }
      .leftMap(_.prettyPrint())
      .flatMap { config =>
        val overlaps = config.formats.findOverlaps
        val message =
          s"Following schema criterions overlap in different groups (TSV, JSON, skip): " +
            s"${overlaps.map(_.asString).mkString(", ")}. " +
            s"Make sure every schema can have only one format"
        Either.cond(overlaps.isEmpty, config, message)
      }

  sealed trait Shredder {
    def output: Shredder.Output
  }
  object Shredder {
    final case class Batch(input: URI, output: Output) extends Shredder
    final case class Stream(input: StreamInput, output: Output, windowing: Duration) extends Shredder

    sealed trait InitPosition
    object InitPosition {
      case object Latest extends InitPosition
      case object TrimHorizon extends InitPosition
      final case class AtTimestamp(timestamp: Instant) extends InitPosition

      implicit val ioCirceInitPositionDecoder: Decoder[InitPosition] =
        Decoder.decodeJson.emap { json =>
          json.asString match {
            case Some("TRIM_HORIZON") => TrimHorizon.asRight
            case Some("LATEST")       => Latest.asRight
            case Some(other) =>
              s"Initial position $other is unknown. Choose from LATEST and TRIM_HORIZON. AT_TIMESTAMP must provide the timestamp".asLeft
            case None =>
              val result = for {
                root <- json.asObject.map(_.toMap)
                atTimestamp <- root.get("AT_TIMESTAMP")
                atTimestampObj <- atTimestamp.asObject.map(_.toMap)
                timestampStr <- atTimestampObj.get("timestamp")
                timestamp <- timestampStr.as[Instant].toOption
              } yield AtTimestamp(timestamp)
              result match {
                case Some(atTimestamp) => atTimestamp.asRight
                case None =>
                  "Initial position can be either LATEST or TRIM_HORIZON string or AT_TIMESTAMP object (e.g. 2020-06-03T00:00:00Z)".asLeft
              }
          }
        }
    }

    sealed trait StreamInput
    object StreamInput {
      case class Kinesis(appName: String, streamName: String, region: String, position: InitPosition) extends StreamInput
      case class File(dir: String) extends StreamInput
    }

    case class Output(path: URI, compression: Compression)

    sealed trait Compression extends StringEnum
    object Compression {
      final case object None extends Compression { val asString = "NONE" }
      final case object Gzip extends Compression { val asString = "GZIP" }
    }
  }

  final case class Formats(default: LoaderMessage.Format,
                           tsv: List[SchemaCriterion],
                           json: List[SchemaCriterion],
                           skip: List[SchemaCriterion]) {
    /** Find if there are overlapping criterions in any two of known three groups */
    def findOverlaps: Set[SchemaCriterion] =
      Formats.findOverlaps(tsv, json) ++
        Formats.findOverlaps(json, skip) ++
        Formats.findOverlaps(skip, tsv)
  }

  object Formats {

    val Default: Formats = Formats(LoaderMessage.Format.TSV, Nil, Nil, Nil)

    /** Find all criterion overlaps in two lists */
    def findOverlaps(as: List[SchemaCriterion], bs: List[SchemaCriterion]): Set[SchemaCriterion] =
      as.flatMap(a => bs.map(b => (a, b))).foldLeft(Set.empty[SchemaCriterion])(aggregateMatching(overlap))

    /** Check if two criterions can have a potential overlap, i.e. a schema belongs to two groups */
    def overlap(a: SchemaCriterion, b: SchemaCriterion): Boolean = (a, b) match {
      case (SchemaCriterion(av, an, _, am, ar, aa), SchemaCriterion(bv, bn, _, bm, br, ba)) =>
        av == bv && an == bn && versionOverlap(am, bm) && versionOverlap(ar, br) && versionOverlap(aa, ba)
    }

    /** Check if two version numbers (MODEL, REVISION or ADDITION) can overlap */
    private def versionOverlap(av: Option[Int], bv: Option[Int]): Boolean = (av, bv) match {
      case (Some(aam), Some(bbm)) if aam == bbm => true // Identical and explicit - overlap
      case (Some(_), Some(_)) => false                  // Different and explicit
      case _ => true                                    // At least one is a wildcard - overlap
    }

    /** Accumulate all pairs matching predicate */
    def aggregateMatching[A](predicate: (A, A) => Boolean)(acc: Set[A], pair: (A, A)): Set[A] = (acc, pair) match {
      case (acc, (a, b)) if predicate(a, b) => acc + a + b
      case (acc, _) => acc
    }
  }

  final case class Monitoring(snowplow: Option[SnowplowMonitoring], sentry: Option[Sentry], metrics: Option[Metrics], alerts: Option[OpsGenie])
  final case class SnowplowMonitoring(appId: String, collector: String)
  final case class Sentry(dsn: URI)
  final case class Metrics(statsd: Option[StatsD], stdout: Option[Stdout])
  // TODO: Refer to https://docs.opsgenie.com/docs/opsgenie-java-api#create-alert for what needs to be configurable
  final case class OpsGenie(apiKey: String, aliasPrefix: String)
  final case class StatsD(hostname: String, port: Int, tags: Map[String, String], prefix: Option[String])
  final case class Stdout(prefix: Option[String])

  implicit val batchShredderDecoder: Decoder[Shredder.Batch] =
    deriveDecoder[Shredder.Batch]
  implicit val streamShredderDecoder: Decoder[Shredder.Stream] =
    deriveDecoder[Shredder.Stream]
  implicit val shredderOutputDecoder: Decoder[Shredder.Output] =
    deriveDecoder[Shredder.Output]
  implicit val shredderFileInputDecoder: Decoder[Shredder.StreamInput.File] =
    deriveDecoder[Shredder.StreamInput.File]
  implicit val shredderKinesisInputDecoder: Decoder[Shredder.StreamInput.Kinesis] =
    deriveDecoder[Shredder.StreamInput.Kinesis]

  implicit val shredderStreamInputDecoder: Decoder[Shredder.StreamInput] =
    Decoder.instance { cur =>
      val typeCur = cur.downField("type")
      typeCur.as[String].map(_.toLowerCase) match {
        case Right("file") =>
          cur.as[Shredder.StreamInput.File]
        case Right("kinesis") =>
          cur.as[Shredder.StreamInput.Kinesis]
        case Right(other) =>
          Left(DecodingFailure(s"Shredder input type $other is not supported yet. Supported types: 'kinesis', 's3' and 'file'", typeCur.history))
        case Left(DecodingFailure(_, List(CursorOp.DownField("type")))) =>
          Left(DecodingFailure("Cannot find 'type' string in shredder configuration", typeCur.history))
        case Left(other) =>
          Left(other)
      }
    }

  implicit val shredderDecoder: Decoder[Shredder] =
    Decoder.instance { cur =>
      val typeCur = cur.downField("type")
      typeCur.as[String].map(_.toLowerCase) match {
        case Right("batch") =>
          cur.as[Shredder.Batch]
        case Right("stream") =>
          cur.as[Shredder.Stream]
        case Right(other) =>
          Left(DecodingFailure(s"Shredder input type $other is not supported yet. Supported types: 'batch' and 'stream'", typeCur.history))
        case Left(DecodingFailure(_, List(CursorOp.DownField("type")))) =>
          Left(DecodingFailure("Cannot find 'type' string in shredder configuration", typeCur.history))
        case Left(other) =>
          Left(other)
      }
    }

  implicit val durationDecoder: Decoder[Duration] =
    Decoder[String].emap(s => Either.catchOnly[NumberFormatException](Duration(s)).leftMap(_.toString))
  implicit val uriDecoder: Decoder[URI] =
    Decoder[String].emap(s => Either.catchOnly[IllegalArgumentException](URI.create(s)).leftMap(_.toString))

  implicit val formatsDecoder: Decoder[Formats] =
    deriveDecoder[Formats]

  implicit val snowplowMonitoringDecoder: Decoder[SnowplowMonitoring] =
    deriveDecoder[SnowplowMonitoring]

  implicit val sentryDecoder: Decoder[Sentry] =
    deriveDecoder[Sentry]

  implicit val statsdDecoder: Decoder[StatsD] =
    deriveDecoder[StatsD]

  implicit val stdoutDecoder: Decoder[Stdout] =
    deriveDecoder[Stdout]

  implicit val metricsDecoder: Decoder[Metrics] =
    deriveDecoder[Metrics]

  implicit val opsGenieDecoder: Decoder[OpsGenie] =
    deriveDecoder[OpsGenie]

  implicit val monitoringDecoder: Decoder[Monitoring] =
    deriveDecoder[Monitoring]

  implicit val decodeOutputCompression: Decoder[Shredder.Compression] =
    StringEnum.decodeStringEnum[Shredder.Compression]
  implicit val encodeOutputCompression: Encoder[Shredder.Compression] =
    Encoder.instance(_.toString.toUpperCase.asJson)

  implicit val configDecoder: Decoder[Config[StorageTarget]] =
    deriveDecoder[Config[StorageTarget]]

  val formats: Lens[Config[StorageTarget], Formats] = GenLens[Config[StorageTarget]](_.formats)
  val monitoring: Lens[Config[StorageTarget], Monitoring] = GenLens[Config[StorageTarget]](_.monitoring)
  val jsonpaths: Lens[Config[StorageTarget], Option[S3.Folder]] = GenLens[Config[StorageTarget]](_.jsonpaths)
}
