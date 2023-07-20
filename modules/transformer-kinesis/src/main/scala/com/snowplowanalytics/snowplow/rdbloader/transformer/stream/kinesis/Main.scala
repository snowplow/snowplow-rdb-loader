/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kinesis

import cats.Parallel
import cats.effect._
import com.snowplowanalytics.snowplow.rdbloader.aws.KinesisProducer.{BackoffPolicy, RequestLimits}
import com.snowplowanalytics.snowplow.rdbloader.aws._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.{Config, Run}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kinesis.generated.BuildInfo

import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s.ceTracking

object Main extends IOApp {
  final val QueueMessageGroupId = "shredding"

  def run(args: List[String]): IO[ExitCode] =
    Run.run[IO, KinesisCheckpointer[IO]](
      args,
      BuildInfo.name,
      BuildInfo.version,
      BuildInfo.description,
      runtime.compute,
      mkSource,
      c => mkSink(c),
      c => mkBadQueue(c),
      mkQueue,
      KinesisCheckpointer.checkpointer
    )

  private def mkSource[F[_]: Async](
    streamInput: Config.StreamInput,
    monitoring: Config.Monitoring
  ): Resource[F, Queue.Consumer[F]] =
    streamInput match {
      case conf: Config.StreamInput.Kinesis =>
        Kinesis.consumer[F](
          conf.appName,
          conf.streamName,
          conf.region,
          conf.position,
          conf.retrievalMode,
          conf.bufferSize,
          conf.customEndpoint,
          conf.dynamodbCustomEndpoint,
          conf.cloudwatchCustomEndpoint,
          monitoring.metrics.cloudwatch
        )
      case _ =>
        Resource.eval(Async[F].raiseError(new IllegalArgumentException(s"Input is not Kinesis")))
    }

  private def mkSink[F[_]: Async](output: Config.Output): Resource[F, BlobStorage[F]] =
    output match {
      case s3Output: Config.Output.S3 =>
        S3.blobStorage[F](s3Output.region.name)
      case _ =>
        Resource.eval(Async[F].raiseError(new IllegalArgumentException(s"Output is not S3")))
    }

  private def mkBadQueue[F[_]: Async: Parallel](
    output: Config.Output.Bad.Queue
  ): Resource[F, Queue.ChunkProducer[F]] =
    output match {
      case kinesis: Config.Output.Bad.Queue.Kinesis =>
        val errorPolicy =
          BackoffPolicy(kinesis.backoffPolicy.minBackoff, kinesis.backoffPolicy.maxBackoff, kinesis.backoffPolicy.maxRetries)

        val throttlingPolicy = BackoffPolicy(
          kinesis.throttledBackoffPolicy.minBackoff,
          kinesis.throttledBackoffPolicy.maxBackoff,
          kinesis.throttledBackoffPolicy.maxRetries
        )

        KinesisProducer.producer[F](
          kinesis.streamName,
          kinesis.region,
          kinesis.customEndpoint,
          errorPolicy,
          throttlingPolicy,
          RequestLimits(kinesis.recordLimit, kinesis.byteLimit)
        )
      case _ =>
        Resource.eval(Async[F].raiseError(new IllegalArgumentException(s"Output queue is not Kinesis")))
    }

  private def mkQueue[F[_]: Async](queueConfig: Config.QueueConfig): Resource[F, Queue.Producer[F]] =
    queueConfig match {
      case Config.QueueConfig.SQS(queueName, region) =>
        SQS.producer(queueName, region.name, QueueMessageGroupId)
      case Config.QueueConfig.SNS(topicArn, region) =>
        SNS.producer(topicArn, region.name, QueueMessageGroupId)
      case _ =>
        Resource.eval(Async[F].raiseError(new IllegalArgumentException(s"Message queue is not SQS or SNS")))
    }
}
