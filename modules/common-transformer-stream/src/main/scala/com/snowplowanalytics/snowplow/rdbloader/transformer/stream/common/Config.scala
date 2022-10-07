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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import java.net.URI

import cats.implicits._
import cats.data.EitherT
import cats.effect.Sync

import io.circe._
import io.circe.generic.semiauto._

import scala.concurrent.duration.{Duration, FiniteDuration}

import com.snowplowanalytics.snowplow.rdbloader.common.config.{ConfigUtils, TransformerConfig}
import com.snowplowanalytics.snowplow.rdbloader.common.config.implicits._
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.{Kinesis => AWSKinesis}
import com.snowplowanalytics.snowplow.rdbloader.common.config.Region

final case class Config(input: Config.StreamInput,
                        windowing: Duration,
                        output: Config.Output,
                        queue: Config.QueueConfig,
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
                             position: AWSKinesis.InitPosition,
                             retrievalMode: AWSKinesis.Retrieval,
                             bufferSize: Int,
                             customEndpoint: Option[URI],
                             dynamodbCustomEndpoint: Option[URI],
                             cloudwatchCustomEndpoint: Option[URI]) extends StreamInput

    final case class File(dir: String) extends StreamInput

    final case class Pubsub(subscription: String,
                            customPubsubEndpoint: Option[String],
                            parallelPullCount: Int,
                            bufferSize: Int,
                            maxAckExtensionPeriod: FiniteDuration,
                            maxOutstandingMessagesSize: Option[Long]) extends StreamInput {
      val (projectId, subscriptionId) =
        subscription.split("/").toList match {
          case List("projects", project, "subscriptions", name) =>
            (project, name)
          case _ =>
            throw new IllegalArgumentException(s"Subscription format $subscription invalid")
        }
    }
  }

  sealed trait Output {
    def path: URI
    def compression: Compression
    def bufferSize: Int
  }

  object Output {
    final case class S3(path: URI, compression: Compression, bufferSize: Int, region: Region) extends Output

    final case class GCS(path: URI, compression: Compression, bufferSize: Int) extends Output

    final case class File(path: URI, compression: Compression, bufferSize: Int) extends Output
  }

  sealed trait QueueConfig extends Product with Serializable

  object QueueConfig {
    final case class SNS(topicArn: String, region: Region) extends QueueConfig

    final case class SQS(queueName: String, region: Region) extends QueueConfig

    final case class Pubsub(topic: String,
                            batchSize: Long,
                            requestByteThreshold: Option[Long],
                            delayThreshold: FiniteDuration) extends QueueConfig {
      val (projectId, topicId) =
        topic.split("/").toList match {
          case List("projects", project, "topics", name) =>
            (project, name)
          case _ =>
            throw new IllegalArgumentException(s"Subscription format $topic invalid")
        }
    }
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
          case Right("pubsub") =>
            cur.as[StreamInput.Pubsub]
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

    implicit val streamInputFileConfigDecoder: Decoder[StreamInput.File] =
      deriveDecoder[StreamInput.File]

    implicit val streamInputPubsubConfigDecoder: Decoder[StreamInput.Pubsub] =
      deriveDecoder[StreamInput.Pubsub]

    implicit val outputConfigDecoder: Decoder[Output] =
      Decoder.instance { cur =>
        val pathCur = cur.downField("path")
        pathCur.as[URI].map(_.getScheme) match {
          case Right("s3" | "s3a" | "s3n") =>
            cur.as[Output.S3]
          case Right("gs") =>
            cur.as[Output.GCS]
          case Right("file") =>
            cur.as[Output.File]
          case Right(other) =>
            Left(DecodingFailure(s"Output type $other is not supported yet. Supported types: 's3', 's3a', 's3n', and 'gs'", pathCur.history))
          case Left(DecodingFailure(_, List(CursorOp.DownField("type")))) =>
            Left(DecodingFailure("Cannot find 'path' string in output configuration", pathCur.history))
          case Left(other) =>
            Left(other)
        }
      }

    implicit val outputS3ConfigDecoder: Decoder[Output.S3] =
      deriveDecoder[Output.S3]

    implicit val outputGCSConfigDecoder: Decoder[Output.GCS] =
      deriveDecoder[Output.GCS]

    implicit val outputFileConfigDecoder: Decoder[Output.File] =
      deriveDecoder[Output.File]

    implicit val queueConfigDecoder: Decoder[QueueConfig] =
      Decoder.instance { cur =>
        val typeCur = cur.downField("type")
        typeCur.as[String].map(_.toLowerCase) match {
          case Right("sns") =>
            cur.as[QueueConfig.SNS]
          case Right("sqs") =>
            cur.as[QueueConfig.SQS]
          case Right("pubsub") =>
            cur.as[QueueConfig.Pubsub]
          case Right(other) =>
            Left(DecodingFailure(s"Queue type $other is not supported yet. Supported types: 'SNS', 'SQS' and 'pubsub'", typeCur.history))
          case Left(DecodingFailure(_, List(CursorOp.DownField("type")))) =>
            Left(DecodingFailure("Cannot find 'type' string in transformer configuration", typeCur.history))
          case Left(other) =>
            Left(other)
        }
      }

    implicit val snsConfigDecoder: Decoder[QueueConfig.SNS] =
      deriveDecoder[QueueConfig.SNS]

    implicit val sqsConfigDecoder: Decoder[QueueConfig.SQS] =
      deriveDecoder[QueueConfig.SQS]

    implicit val pubsubConfigDecoder: Decoder[QueueConfig.Pubsub] =
      deriveDecoder[QueueConfig.Pubsub]

    implicit val configDecoder: Decoder[Config] =
      deriveDecoder[Config]

  }

  def impureDecoders: Decoders = new Decoders with TransformerConfig.ImpureRegionDecodable


}
