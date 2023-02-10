/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kinesis

import cats.Parallel
import cats.effect._
import com.snowplowanalytics.snowplow.rdbloader.aws.KinesisProducer.{BackoffPolicy, RequestLimits}
import com.snowplowanalytics.snowplow.rdbloader.aws._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.{Config, Run}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kinesis.generated.BuildInfo

object Main extends IOApp {
  final val QueueMessageGroupId = "shredding"

  def run(args: List[String]): IO[ExitCode] =
    Run.run[IO, KinesisCheckpointer[IO]](
      args,
      BuildInfo.name,
      BuildInfo.version,
      BuildInfo.description,
      executionContext,
      mkSource,
      (_, c) => mkSink(c),
      (blocker, c) => mkBadQueue(blocker, c),
      mkQueue,
      KinesisCheckpointer.checkpointer
    )

  private def mkSource[F[_]: ConcurrentEffect: ContextShift: Timer](
    blocker: Blocker,
    streamInput: Config.StreamInput,
    monitoring: Config.Monitoring
  ): Resource[F, Queue.Consumer[F]] =
    streamInput match {
      case conf: Config.StreamInput.Kinesis =>
        Kinesis.consumer[F](
          blocker,
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
        Resource.eval(ConcurrentEffect[F].raiseError(new IllegalArgumentException(s"Input is not Kinesis")))
    }

  private def mkSink[F[_]: ConcurrentEffect: Timer](output: Config.Output): Resource[F, BlobStorage[F]] =
    output match {
      case s3Output: Config.Output.S3 =>
        S3.blobStorage[F](s3Output.region.name)
      case _ =>
        Resource.eval(ConcurrentEffect[F].raiseError(new IllegalArgumentException(s"Output is not S3")))
    }

  private def mkBadQueue[F[_]: ConcurrentEffect: Timer: ContextShift: Parallel](
    blocker: Blocker,
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
          blocker,
          errorPolicy,
          throttlingPolicy,
          RequestLimits(kinesis.recordLimit, kinesis.byteLimit)
        )
      case _ =>
        Resource.eval(ConcurrentEffect[F].raiseError(new IllegalArgumentException(s"Output queue is not Kinesis")))
    }

  private def mkQueue[F[_]: ConcurrentEffect](queueConfig: Config.QueueConfig): Resource[F, Queue.Producer[F]] =
    queueConfig match {
      case Config.QueueConfig.SQS(queueName, region) =>
        SQS.producer(queueName, region.name, QueueMessageGroupId)
      case Config.QueueConfig.SNS(topicArn, region) =>
        SNS.producer(topicArn, region.name, QueueMessageGroupId)
      case _ =>
        Resource.eval(ConcurrentEffect[F].raiseError(new IllegalArgumentException(s"Message queue is not SQS or SNS")))
    }
}
