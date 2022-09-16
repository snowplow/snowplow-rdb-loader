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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.pubsub

import blobstore.gcs.GcsStore
import scala.concurrent.duration._
import cats.effect._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.Logger
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Config
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.pubsub.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Run
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.gcp.{GCS, Pubsub}

object Main extends IOApp {

  implicit private def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def run(args: List[String]): IO[ExitCode] =
    Run.run[IO, PubsubCheckpointer[IO]](
      args,
      BuildInfo.name,
      BuildInfo.version,
      BuildInfo.description,
      executionContext,
      (b, s, _) => mkSource(b, s),
      mkSink,
      mkQueue,
      PubsubCheckpointer.checkpointer
    )

  private def mkSource[F[_] : ConcurrentEffect : ContextShift : Timer](blocker: Blocker,
                                                                       streamInput: Config.StreamInput): Resource[F, Queue.Consumer[F]] =
    streamInput match {
      case conf: Config.StreamInput.Pubsub =>
        Pubsub.consumer(
          blocker,
          conf.projectId,
          conf.subscriptionId,
          parallelPullCount = 1,
          maxQueueSize = 100000,
          maxAckExtensionPeriod = 2.hours,
          customPubsubEndpoint = None
        )
      case _ =>
        Resource.eval(ConcurrentEffect[F].raiseError(new IllegalArgumentException(s"Input is not Pubsub")))
    }

  private def mkSink[F[_]: ConcurrentEffect: Timer: ContextShift](blocker: Blocker, output: Config.Output): Resource[F, BlobStorage[F]] =
    output match {
      case _: Config.Output.GCS =>
        for {
          client <- Resource.pure[F, GcsStore[F]](GCS.getClient[F](blocker))
          blobStorage = GCS.blobStorage[F](client)
        } yield blobStorage
      case _ =>
        Resource.eval(ConcurrentEffect[F].raiseError(new IllegalArgumentException(s"Output is not GCS")))
    }

  private def mkQueue[F[_]: ConcurrentEffect](queueConfig: Config.QueueConfig): Resource[F, Queue.Producer[F]] =
    queueConfig match {
      case Config.QueueConfig.Pubsub(projectId, topicId) =>
        Pubsub.producer(projectId, topicId)
      case _ =>
        Resource.eval(ConcurrentEffect[F].raiseError(new IllegalArgumentException(s"Message queue is not Pubsub")))
    }
}
