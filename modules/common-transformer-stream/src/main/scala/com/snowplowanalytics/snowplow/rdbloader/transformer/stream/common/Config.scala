/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import java.net.URI
import cats.implicits._
import cats.data.EitherT
import cats.effect.Sync
import com.snowplowanalytics.snowplow.rdbloader.common.config.args.HoconOrPath
import io.circe._
import io.circe.generic.semiauto._

import scala.concurrent.duration.{Duration, FiniteDuration}
import com.snowplowanalytics.snowplow.rdbloader.common.telemetry.Telemetry
import com.snowplowanalytics.snowplow.rdbloader.common.config.{ConfigUtils, TransformerConfig}
import com.snowplowanalytics.snowplow.rdbloader.common.config.implicits._
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.{Kinesis => AWSKinesis}
import com.snowplowanalytics.snowplow.rdbloader.common.config.Region
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Config.Output.Bad

final case class Config(
  input: Config.StreamInput,
  windowing: Duration,
  output: Config.Output,
  queue: Config.QueueConfig,
  formats: TransformerConfig.Formats,
  monitoring: Config.Monitoring,
  telemetry: Telemetry.Config,
  featureFlags: TransformerConfig.FeatureFlags,
  validations: TransformerConfig.Validations
)

object Config {

  def parse[F[_]: Sync](config: HoconOrPath): EitherT[F, String, Config] =
    parse(config, impureDecoders)

  def parse[F[_]: Sync](config: HoconOrPath, decoders: Decoders): EitherT[F, String, Config] = {
    import decoders._
    for {
      config <- ConfigUtils.parseAppConfigF[F, Config](config)
      _ <- EitherT.fromEither[F](TransformerConfig.formatsCheck(config.formats))
    } yield config
  }

  sealed trait StreamInput extends Product with Serializable
  object StreamInput {
    final case class Kinesis(
      appName: String,
      streamName: String,
      region: Region,
      position: AWSKinesis.InitPosition,
      retrievalMode: AWSKinesis.Retrieval,
      bufferSize: Int,
      customEndpoint: Option[URI],
      dynamodbCustomEndpoint: Option[URI],
      cloudwatchCustomEndpoint: Option[URI]
    ) extends StreamInput

    final case class Pubsub(
      subscription: String,
      customPubsubEndpoint: Option[String],
      parallelPullCount: Int,
      bufferSize: Int,
      maxAckExtensionPeriod: FiniteDuration,
      maxOutstandingMessagesSize: Option[Long]
    ) extends StreamInput {
      val (projectId, subscriptionId) =
        subscription.split("/").toList match {
          case List("projects", project, "subscriptions", name) =>
            (project, name)
          case _ =>
            throw new IllegalArgumentException(s"Subscription format $subscription invalid")
        }
    }

    final case class Kafka(
      topicName: String,
      bootstrapServers: String,
      consumerConf: Map[String, String]
    ) extends StreamInput
  }

  sealed trait Output {
    def path: URI
    def compression: Compression
    def bufferSize: Int
    def maxRecordsPerFile: Long
    def bad: Bad
  }

  object Output {
    final case class S3(
      path: URI,
      compression: Compression,
      bufferSize: Int,
      region: Region,
      maxRecordsPerFile: Long,
      bad: Bad
    ) extends Output

    final case class GCS(
      path: URI,
      compression: Compression,
      bufferSize: Int,
      maxRecordsPerFile: Long,
      bad: Bad
    ) extends Output

    final case class AzureBlobStorage(
      path: URI,
      compression: Compression,
      bufferSize: Int,
      maxRecordsPerFile: Long,
      bad: Bad
    ) extends Output

    sealed trait Bad
    object Bad {

      final case object File extends Bad

      sealed trait Queue extends Bad

      object Queue {
        final case class Kinesis(
          streamName: String,
          region: Region,
          recordLimit: Int,
          byteLimit: Int,
          backoffPolicy: Kinesis.BackoffPolicy,
          throttledBackoffPolicy: Kinesis.BackoffPolicy,
          customEndpoint: Option[URI]
        ) extends Queue

        object Kinesis {
          case class BackoffPolicy(
            minBackoff: FiniteDuration,
            maxBackoff: FiniteDuration,
            maxRetries: Option[Int]
          )
        }

        final case class Pubsub(
          topic: String,
          batchSize: Long,
          requestByteThreshold: Option[Long],
          delayThreshold: FiniteDuration
        ) extends Queue {
          val (projectId, topicId) =
            topic.split("/").toList match {
              case List("projects", project, "topics", name) =>
                (project, name)
              case _ =>
                throw new IllegalArgumentException(s"Subscription format $topic invalid")
            }
        }

        final case class Kafka(
          topicName: String,
          bootstrapServers: String,
          producerConf: Map[String, String]
        ) extends Queue
      }

    }
  }

  sealed trait QueueConfig extends Product with Serializable

  object QueueConfig {
    final case class SNS(topicArn: String, region: Region) extends QueueConfig

    final case class SQS(queueName: String, region: Region) extends QueueConfig

    final case class Pubsub(
      topic: String,
      batchSize: Long,
      requestByteThreshold: Option[Long],
      delayThreshold: FiniteDuration
    ) extends QueueConfig {
      val (projectId, topicId) =
        topic.split("/").toList match {
          case List("projects", project, "topics", name) =>
            (project, name)
          case _ =>
            throw new IllegalArgumentException(s"Subscription format $topic invalid")
        }
    }

    final case class Kafka(
      topicName: String,
      bootstrapServers: String,
      producerConf: Map[String, String]
    ) extends QueueConfig
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

  trait Decoders extends TransformerConfig.Decoders {

    implicit val streamInputConfigDecoder: Decoder[StreamInput] =
      Decoder.instance { cur =>
        val typeCur = cur.downField("type")
        typeCur.as[String].map(_.toLowerCase) match {
          case Right("kinesis") =>
            cur.as[StreamInput.Kinesis]
          case Right("pubsub") =>
            cur.as[StreamInput.Pubsub]
          case Right("kafka") =>
            cur.as[StreamInput.Kafka]
          case Right(other) =>
            Left(
              DecodingFailure(
                s"Shredder input type $other is not supported yet. Supported types: 'kinesis', 'pubsub', 'kafka'",
                typeCur.history
              )
            )
          case Left(DecodingFailure(_, List(CursorOp.DownField("type")))) =>
            Left(DecodingFailure("Cannot find 'type' string in transformer configuration", typeCur.history))
          case Left(other) =>
            Left(other)
        }
      }

    implicit val streamInputKinesisConfigDecoder: Decoder[StreamInput.Kinesis] =
      deriveDecoder[StreamInput.Kinesis]

    implicit val streamInputPubsubConfigDecoder: Decoder[StreamInput.Pubsub] =
      deriveDecoder[StreamInput.Pubsub]

    implicit val streamInputKafkaConfigDecoder: Decoder[StreamInput.Kafka] =
      deriveDecoder[StreamInput.Kafka]

    implicit val outputConfigDecoder: Decoder[Output] =
      Decoder.instance { cur =>
        val pathCur = cur.downField("path")
        pathCur.as[URI].map(_.getScheme) match {
          case Right("s3" | "s3a" | "s3n") =>
            cur.as[Output.S3]
          case Right("gs") =>
            cur.as[Output.GCS]
          case Right("http") | Right("https") =>
            cur.as[Output.AzureBlobStorage]
          case Right(other) =>
            Left(
              DecodingFailure(
                s"Output type $other is not supported yet. Supported types: 's3', 's3a', 's3n', 'gs', 'http', 'https'",
                pathCur.history
              )
            )
          case Left(DecodingFailure(_, List(CursorOp.DownField("type")))) =>
            Left(DecodingFailure("Cannot find 'path' string in output configuration", pathCur.history))
          case Left(other) =>
            Left(other)
        }
      }

    implicit val kinesisBadOutputConfigDecoder: Decoder[Output.Bad.Queue.Kinesis] =
      deriveDecoder[Output.Bad.Queue.Kinesis]

    implicit val pubsubBadOutputConfigDecoder: Decoder[Output.Bad.Queue.Pubsub] =
      deriveDecoder[Output.Bad.Queue.Pubsub]

    implicit val kafkaBadOutputConfigDecoder: Decoder[Output.Bad.Queue.Kafka] =
      deriveDecoder[Output.Bad.Queue.Kafka]

    implicit val badOutputConfigDecoder: Decoder[Output.Bad] =
      Decoder.instance { cur =>
        val typeCur = cur.downField("type")
        typeCur.as[String].map(_.toLowerCase) match {
          case Right("kinesis") =>
            cur.as[Output.Bad.Queue.Kinesis]
          case Right("pubsub") =>
            cur.as[Output.Bad.Queue.Pubsub]
          case Right("kafka") =>
            cur.as[Output.Bad.Queue.Kafka]
          case Right("file") =>
            Right(Output.Bad.File)
          case Right(other) =>
            Left(
              DecodingFailure(
                s"Bad output type '$other' is not supported yet. Supported types: 'kinesis', 'pubsub', 'kafka', 'file'",
                typeCur.history
              )
            )
          case Left(other) =>
            Left(other)
        }
      }
    implicit val outputS3ConfigDecoder: Decoder[Output.S3] =
      deriveDecoder[Output.S3]

    implicit val outputGCSConfigDecoder: Decoder[Output.GCS] =
      deriveDecoder[Output.GCS]

    implicit val outputAzureBlobStorageConfigDecoder: Decoder[Output.AzureBlobStorage] =
      deriveDecoder[Output.AzureBlobStorage]

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
          case Right("kafka") =>
            cur.as[QueueConfig.Kafka]
          case Right(other) =>
            Left(
              DecodingFailure(
                s"Queue type $other is not supported yet. Supported types: 'SNS', 'SQS', 'pubsub' and 'kafka'",
                typeCur.history
              )
            )
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

    implicit val kafkaConfigDecoder: Decoder[QueueConfig.Kafka] =
      deriveDecoder[QueueConfig.Kafka]

    implicit val configDecoder: Decoder[Config] =
      deriveDecoder[Config].ensure(validateConfig)

    implicit val backoffPolicyDecoder: Decoder[Output.Bad.Queue.Kinesis.BackoffPolicy] =
      deriveDecoder[Output.Bad.Queue.Kinesis.BackoffPolicy]

  }

  def impureDecoders: Decoders = new Decoders with TransformerConfig.ImpureRegionDecodable

  private def validateConfig(config: Config): List[String] =
    List(
      TransformerConfig.formatsCheck(config.formats).swap.map(List(_)).getOrElse(List.empty)
    ).flatten
}
