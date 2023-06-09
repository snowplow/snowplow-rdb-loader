/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import java.net.URI
import java.time.Instant

import cats.implicits._

import io.circe._
import io.circe.generic.semiauto._

import scala.concurrent.duration.FiniteDuration

import com.snowplowanalytics.snowplow.rdbloader.common.Common
import com.snowplowanalytics.snowplow.rdbloader.common.config.{ConfigUtils, TransformerConfig}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.Region
import com.snowplowanalytics.snowplow.rdbloader.common.config.implicits._

final case class Config(
  input: URI,
  output: Config.Output,
  queue: Config.QueueConfig,
  formats: TransformerConfig.Formats,
  monitoring: Config.Monitoring,
  deduplication: Config.Deduplication,
  runInterval: Config.RunInterval,
  featureFlags: TransformerConfig.FeatureFlags,
  validations: TransformerConfig.Validations
)

object Config {
  def fromString(conf: String): Either[String, Config] =
    fromString(conf, impureDecoders)

  def fromString(conf: String, decoders: Decoders): Either[String, Config] = {
    import decoders._
    for {
      config <- ConfigUtils.fromString[Config](conf)
      _ <- TransformerConfig.formatsCheck(config.formats)
    } yield config
  }

  final case class Output(
    path: URI,
    compression: Compression,
    region: Region,
    maxRecordsPerFile: Long,
    maxBadBufferSize: Int,
    bad: Output.BadSink
  )

  object Output {

    sealed trait BadSink
    object BadSink {

      final case class Kinesis(
        streamName: String,
        region: Region,
        recordLimit: Int,
        byteLimit: Int,
        backoffPolicy: BackoffPolicy,
        throttledBackoffPolicy: BackoffPolicy
      ) extends BadSink

      case object File extends BadSink

      final case class BackoffPolicy(
        minBackoff: FiniteDuration,
        maxBackoff: FiniteDuration,
        maxRetries: Option[Int]
      )
    }
  }

  sealed trait QueueConfig extends Product with Serializable

  object QueueConfig {
    final case class SNS(topicArn: String, region: Region) extends QueueConfig

    final case class SQS(queueName: String, region: Region) extends QueueConfig
  }

  final case class RunInterval(
    sinceTimestamp: Option[RunInterval.IntervalInstant],
    sinceAge: Option[FiniteDuration],
    until: Option[RunInterval.IntervalInstant]
  )

  object RunInterval {
    final case class IntervalInstant(value: Instant)

    implicit val runIntervalInstantConfigDecoder: Decoder[IntervalInstant] =
      Decoder[String].emap(v => Common.parseFolderTime(v).leftMap(_.toString).map(IntervalInstant))

    implicit val runIntervalDecoder: Decoder[RunInterval] =
      deriveDecoder[RunInterval]
  }

  final case class Monitoring(sentry: Option[TransformerConfig.Sentry])
  object Monitoring {
    implicit val monitoringDecoder: Decoder[Monitoring] =
      deriveDecoder[Monitoring]
  }

  final case class Deduplication(synthetic: Deduplication.Synthetic, natural: Boolean)

  object Deduplication {

    /**
     * Configuration for in-batch synthetic deduplication
     */
    sealed trait Synthetic
    object Synthetic {
      final case object Join extends Synthetic
      final case class Broadcast(cardinality: Int) extends Synthetic
      final case object None extends Synthetic

      implicit val ioCirceSyntheticDecoder: Decoder[Synthetic] =
        Decoder.instance { cur =>
          val typeCur = cur.downField("type")
          typeCur.as[String].map(_.toLowerCase) match {
            case Right("none") =>
              Right(None)
            case Right("join") =>
              Right(Join)
            case Right("broadcast") =>
              cur.downField("cardinality").as[Int].map(Broadcast.apply)
            case Right(other) =>
              Left(DecodingFailure(s"Type $other is unknown for synthetic deduplication", cur.history))
            case Left(other) =>
              Left(other)
          }
        }
      implicit val ioCirceSyntheticEncoder: Encoder[Synthetic] =
        deriveEncoder[Synthetic]
    }

    implicit val ioCirceDeduplicationDecoder: Decoder[Deduplication] =
      deriveDecoder[Deduplication]
    implicit val ioCirceDeduplicationEncoder: Encoder[Deduplication] =
      deriveEncoder[Deduplication]
  }

  trait Decoders extends TransformerConfig.Decoders {
    implicit val outputConfigDecoder: Decoder[Output] =
      deriveDecoder[Output]

    implicit val queueConfigDecoder: Decoder[QueueConfig] =
      Decoder.instance { cur =>
        val typeCur = cur.downField("type")
        typeCur.as[String].map(_.toLowerCase) match {
          case Right("sns") =>
            cur.as[QueueConfig.SNS]
          case Right("sqs") =>
            cur.as[QueueConfig.SQS]
          case Right(other) =>
            Left(DecodingFailure(s"Queue type $other is not supported yet. Supported types: 'SNS' and 'SQS'", typeCur.history))
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

    implicit val backoffPolicyDecoder: Decoder[Output.BadSink.BackoffPolicy] =
      deriveDecoder[Output.BadSink.BackoffPolicy]

    implicit val kinesisConfigDecoder: Decoder[Output.BadSink.Kinesis] =
      deriveDecoder[Output.BadSink.Kinesis]

    implicit val badSinkConfigDecoder: Decoder[Output.BadSink] =
      Decoder.instance { cur =>
        val typeCur = cur.downField("type")
        typeCur.as[String].map(_.toLowerCase) match {
          case Right("kinesis") =>
            cur.as[Output.BadSink.Kinesis]
          case Right("file") =>
            Right(Output.BadSink.File)
          case Right(other) =>
            Left(DecodingFailure(s"Bad output type '$other' is not supported yet. Supported types: 'kinesis', 'file'", typeCur.history))
          case Left(other) =>
            Left(other)
        }
      }
    implicit val configDecoder: Decoder[Config] =
      deriveDecoder[Config]

  }

  def impureDecoders: Decoders = new Decoders with TransformerConfig.ImpureRegionDecodable

}
