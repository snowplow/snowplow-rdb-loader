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

import cats.effect._

import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.Logger

import com.google.api.gax.batching.FlowControlSettings
import com.google.api.gax.core.ExecutorProvider
import com.google.common.util.concurrent.{ForwardingListeningExecutorService, MoreExecutors}

import java.util.concurrent.{Callable, ScheduledExecutorService, ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.gcp.{GCS, Pubsub}

import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Config
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.pubsub.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Run

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
          parallelPullCount = conf.parallelPullCount,
          bufferSize = conf.bufferSize,
          maxAckExtensionPeriod = conf.maxAckExtensionPeriod,
          customPubsubEndpoint = conf.customPubsubEndpoint,
          customizeSubscriber = { s =>
            s.setFlowControlSettings({
              val builder = FlowControlSettings.newBuilder()
                // In here, we are only setting request bytes because it is safer choice
                // in term of memory safety.
                // Also, buffer size set above doesn't have to be inline with flow control settings.
                // Even if more items than given buffer size arrives, it wouldn't create problem because
                // incoming items will be blocked until buffer is emptied. However, making buffer too big creates
                // memory problem again.
              (conf.maxOutstandingMessagesSize match {
                case Some(v) => builder.setMaxOutstandingRequestBytes(v * 1000000)
                case None => builder.setMaxOutstandingRequestBytes(null)
              }).build()
            })
            s.setExecutorProvider {
              new ExecutorProvider {
                def shouldAutoClose: Boolean = true
                def getExecutor: ScheduledExecutorService = scheduledExecutorService
              }
            }
          }
        )
      case _ =>
        Resource.eval(ConcurrentEffect[F].raiseError(new IllegalArgumentException(s"Input is not Pubsub")))
    }

  def scheduledExecutorService: ScheduledExecutorService = new ForwardingListeningExecutorService with ScheduledExecutorService {
    val delegate = MoreExecutors.newDirectExecutorService
    lazy val scheduler = new ScheduledThreadPoolExecutor(1) // I think this scheduler is never used, but I implement it here for safety
    override def schedule[V](callable: Callable[V], delay: Long, unit: TimeUnit): ScheduledFuture[V] =
      scheduler.schedule(callable, delay, unit)
    override def schedule(runnable: Runnable, delay: Long, unit: TimeUnit): ScheduledFuture[_] =
      scheduler.schedule(runnable, delay, unit)
    override def scheduleAtFixedRate(runnable: Runnable, initialDelay: Long, period: Long, unit: TimeUnit): ScheduledFuture[_] =
      scheduler.scheduleAtFixedRate(runnable, initialDelay, period, unit)
    override def scheduleWithFixedDelay(runnable: Runnable, initialDelay: Long, delay: Long, unit: TimeUnit): ScheduledFuture[_] =
      scheduler.scheduleWithFixedDelay(runnable, initialDelay, delay, unit)
    override def shutdown(): Unit = {
      delegate.shutdown()
      scheduler.shutdown()
    }
  }

  private def mkSink[F[_]: ConcurrentEffect: Timer: ContextShift](blocker: Blocker, output: Config.Output): Resource[F, BlobStorage[F]] =
    output match {
      case _: Config.Output.GCS =>
        GCS.blobStorage[F](blocker)
      case _ =>
        Resource.eval(ConcurrentEffect[F].raiseError(new IllegalArgumentException(s"Output is not GCS")))
    }

  private def mkQueue[F[_]: ConcurrentEffect](queueConfig: Config.QueueConfig): Resource[F, Queue.Producer[F]] =
    queueConfig match {
      case p: Config.QueueConfig.Pubsub =>
        Pubsub.producer(
          p.projectId,
          p.topicId,
          batchSize = p.batchSize,
          requestByteThreshold = p.requestByteThreshold,
          delayThreshold = p.delayThreshold
        )
      case _ =>
        Resource.eval(ConcurrentEffect[F].raiseError(new IllegalArgumentException(s"Message queue is not Pubsub")))
    }
}
