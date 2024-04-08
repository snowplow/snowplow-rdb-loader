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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kinesis

import cats.Parallel
import cats.effect._
import org.apache.hadoop.conf.Configuration
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import com.snowplowanalytics.snowplow.rdbloader.aws.KinesisProducer.{BackoffPolicy, RequestLimits}
import com.snowplowanalytics.snowplow.rdbloader.aws._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.{Config, Run}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.parquet.ParquetOps
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kinesis.generated.BuildInfo
import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s.ceTracking

import scala.concurrent.duration.DurationInt

object Main extends IOApp {
  final val QueueMessageGroupId = "shredding"

  override def runtimeConfig =
    super.runtimeConfig.copy(cpuStarvationCheckInterval = 10.seconds)

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
      KinesisCheckpointer.checkpointer,
      parquetOps
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

  private def parquetOps: ParquetOps = new ParquetOps {

    override def transformPath(p: String): String =
      ParquetOps.noop.transformPath(p)

    override def hadoopConf: Configuration = {
      val s3Conf = new Configuration()
      s3Conf.set("fs.s3a.endpoint.region", (new DefaultAwsRegionProviderChain).getRegion.id)
      s3Conf
    }

  }
}
