/*
 * Copyright (c) 2021-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis

import java.time.Instant
import java.net.URI

import cats.implicits._
import cats.data.EitherT
import cats.effect.Sync

import io.circe._
import io.circe.generic.semiauto._

import scala.concurrent.duration.{Duration, FiniteDuration}

import com.snowplowanalytics.snowplow.rdbloader.common.config.{ConfigUtils, Region, TransformerConfig}
import com.snowplowanalytics.snowplow.rdbloader.common.config.implicits._

final case class Config(input: Config.StreamInput,
                        windowing: Duration,
                        output: TransformerConfig.Output,
                        queue: TransformerConfig.QueueConfig,
                        formats: TransformerConfig.Formats,
                        monitoring: Config.Monitoring,
                        telemetry: Config.Telemetry,
                        featureFlags: TransformerConfig.FeatureFlags,
                        validations: TransformerConfig.Validations)

object Config {

  def fromString[F[_]: Sync](conf: String): EitherT[F, String, Config] =
    fromString(conf, impureDecoders)

  def fromString[F[_]: Sync](conf: String, decoders: Decoders): EitherT[F, String, Config] = {
    import decoders._
    for {
      config <- ConfigUtils.fromStringF[F, Config](conf)
      _      <- EitherT.fromEither[F](TransformerConfig.formatsCheck(config.formats))
    } yield config
  }

  sealed trait StreamInput extends Product with Serializable
  object StreamInput {
    final case class Kinesis(appName: String,
                             streamName: String,
                             region: Region,
                             position: InitPosition,
                             retrievalMode: Retrieval,
                             bufferSize: Int,
                             customEndpoint: Option[URI],
                             dynamodbCustomEndpoint: Option[URI],
                             cloudwatchCustomEndpoint: Option[URI]
                            ) extends StreamInput

    final case class File(dir: String) extends StreamInput
    object File {
      implicit val fileDecoder: Decoder[File] =
        deriveDecoder[File]
    }
  }

  sealed trait InitPosition extends Product with Serializable
  object InitPosition {
    case object Latest extends InitPosition
    case object TrimHorizon extends InitPosition
    final case class AtTimestamp(timestamp: Instant) extends InitPosition

    implicit val initPositionConfigDecoder: Decoder[InitPosition] =
      Decoder.decodeJson.emap { json =>
        json.asString match {
          case Some("TRIM_HORIZON") => InitPosition.TrimHorizon.asRight
          case Some("LATEST") => InitPosition.Latest.asRight
          case Some(other) =>
            s"Initial position $other is unknown. Choose from LATEST and TRIM_HORIZON. AT_TIMESTAMP must provide the timestamp".asLeft
          case None =>
            val result = for {
              root <- json.asObject.map(_.toMap)
              atTimestamp <- root.get("AT_TIMESTAMP")
              atTimestampObj <- atTimestamp.asObject.map(_.toMap)
              timestampStr <- atTimestampObj.get("timestamp")
              timestamp <- timestampStr.as[Instant].toOption
            } yield InitPosition.AtTimestamp(timestamp)
            result match {
              case Some(atTimestamp) => atTimestamp.asRight
              case None =>
                "Initial position can be either LATEST or TRIM_HORIZON string or AT_TIMESTAMP object (e.g. 2020-06-03T00:00:00Z)".asLeft
            }
        }
      }
  }

  sealed trait Retrieval
  object Retrieval {
    case class Polling(maxRecords: Int) extends Retrieval
    case object FanOut extends Retrieval

    case class RetrievalRaw(`type`: String, maxRecords: Option[Int])
    implicit val retrievalRawDecoder: Decoder[RetrievalRaw] = deriveDecoder[RetrievalRaw]

    implicit val retrievalDecoder: Decoder[Retrieval] =
      Decoder.instance { cur =>
        for {
          rawParsed <- cur.as[RetrievalRaw].map(raw => raw.copy(`type` = raw.`type`.toUpperCase))
          retrieval <- rawParsed match {
            case RetrievalRaw("POLLING", Some(maxRecords)) =>
              Polling(maxRecords).asRight
            case RetrievalRaw("FANOUT", _) =>
              FanOut.asRight
            case other =>
              DecodingFailure(
                s"Retrieval mode $other is not supported. Possible types are FanOut and Polling (must provide maxRecords field)",
                cur.history
              ).asLeft
          }
        } yield retrieval
      }
    implicit val retrievalEncoder: Encoder[Retrieval] = deriveEncoder[Retrieval]
  }

  final case class Monitoring(sentry: Option[TransformerConfig.Sentry], metrics: MetricsReporters)
  object Monitoring {
    implicit val decoder: Decoder[Monitoring] =
      deriveDecoder[Monitoring]
  }

  final case class MetricsReporters(
    statsd: Option[MetricsReporters.StatsD],
    stdout: Option[MetricsReporters.Stdout],
    cloudwatch: Boolean
  )

  object MetricsReporters {
    final case class Stdout(period: FiniteDuration, prefix: Option[String])
    final case class StatsD(
      hostname: String,
      port: Int,
      tags: Map[String, String],
      period: FiniteDuration,
      prefix: Option[String]
    )

    implicit val stdoutDecoder: Decoder[Stdout] =
      deriveDecoder[Stdout].emap { stdout =>
        if (stdout.period < Duration.Zero)
          "metrics report period in config file cannot be less than 0".asLeft
        else
          stdout.asRight
      }

    implicit val statsDecoder: Decoder[StatsD] =
      deriveDecoder[StatsD].emap { statsd =>
        if (statsd.period < Duration.Zero)
          "metrics report period in config file cannot be less than 0".asLeft
        else
          statsd.asRight
      }

    implicit val metricsReportersDecoder: Decoder[MetricsReporters] =
      deriveDecoder[MetricsReporters]
  }

  case class Telemetry(
    disable: Boolean,
    interval: FiniteDuration,
    method: String,
    collectorUri: String,
    collectorPort: Int,
    secure: Boolean,
    userProvidedId: Option[String],
    autoGeneratedId: Option[String],
    instanceId: Option[String],
    moduleName: Option[String],
    moduleVersion: Option[String]
  )

  implicit val telemetryDecoder: Decoder[Telemetry] =
    deriveDecoder[Telemetry]

  trait Decoders extends TransformerConfig.Decoders {

    implicit val streamInputConfigDecoder: Decoder[StreamInput] =
      Decoder.instance { cur =>
        val typeCur = cur.downField("type")
        typeCur.as[String].map(_.toLowerCase) match {
          case Right("file") =>
            cur.as[StreamInput.File]
          case Right("kinesis") =>
            cur.as[StreamInput.Kinesis]
          case Right(other) =>
            Left(DecodingFailure(s"Shredder input type $other is not supported yet. Supported types: 'kinesis', 's3' and 'file'", typeCur.history))
          case Left(DecodingFailure(_, List(CursorOp.DownField("type")))) =>
            Left(DecodingFailure("Cannot find 'type' string in transformer configuration", typeCur.history))
          case Left(other) =>
            Left(other)
        }
      }

    implicit val streamInputKinesisConfigDecoder: Decoder[StreamInput.Kinesis] =
      deriveDecoder[StreamInput.Kinesis]

    implicit val configDecoder: Decoder[Config] =
      deriveDecoder[Config]

  }

  def impureDecoders: Decoders = new Decoders with TransformerConfig.ImpureRegionDecodable


}
