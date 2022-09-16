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

import cats.effect._
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Config
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kinesis.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Run
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{Queue, BlobStorage}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.aws.{Kinesis, S3, SQS, SNS}

object Main extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    Run.run[IO, KinesisCheckpointer[IO]](
      args,
      BuildInfo.name,
      BuildInfo.version,
      BuildInfo.description,
      executionContext,
      mkSource,
      (_, c) => mkSink(c),
      mkQueue,
      KinesisCheckpointer.checkpointer
    )

  private def mkSource[F[_]: ConcurrentEffect: ContextShift: Timer](blocker: Blocker,
                                                                    streamInput: Config.StreamInput,
                                                                    monitoring: Config.Monitoring): Resource[F, Queue.Consumer[F]] =
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
        for {
          client <- Resource.eval(S3.getClient[F](s3Output.region.name))
          blobStorage = S3.blobStorage[F](client)
        } yield blobStorage
      case _ =>
        Resource.eval(ConcurrentEffect[F].raiseError(new IllegalArgumentException(s"Output is not S3")))
    }

  private def mkQueue[F[_]: ConcurrentEffect](queueConfig: Config.QueueConfig): Resource[F, Queue.Producer[F]] =
    queueConfig match {
      case Config.QueueConfig.SQS(queueName, region) =>
        SQS.producer(queueName, region.name)
      case Config.QueueConfig.SNS(topicArn, region) =>
        SNS.producer(topicArn, region.name)
      case _ =>
        Resource.eval(ConcurrentEffect[F].raiseError(new IllegalArgumentException(s"Message queue is not SQS or SNS")))
    }
}
