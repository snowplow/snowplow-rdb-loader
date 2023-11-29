/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.pubsub

import cats.effect._

import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.Logger

import com.google.api.gax.batching.FlowControlSettings
import com.google.api.gax.core.FixedExecutorProvider
import com.google.common.util.concurrent.ForwardingExecutorService

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.gcp.{GCS, Pubsub}

import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Config
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.pubsub.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Run
import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s.ceTracking

import scala.concurrent.duration.DurationInt
import java.util.concurrent.{Callable, ExecutorService, Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}

object Main extends IOApp {

  override def runtimeConfig =
    super.runtimeConfig.copy(cpuStarvationCheckInterval = 10.seconds)

  implicit private def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def run(args: List[String]): IO[ExitCode] =
    Run.run[IO, PubsubCheckpointer[IO]](
      args,
      BuildInfo.name,
      BuildInfo.version,
      BuildInfo.description,
      runtime.compute,
      (s, _) => mkSource(s),
      mkSink,
      mkBadQueue,
      mkQueue,
      PubsubCheckpointer.checkpointer
    )

  private def mkSource[F[_]: Async](
    streamInput: Config.StreamInput
  ): Resource[F, Queue.Consumer[F]] =
    streamInput match {
      case conf: Config.StreamInput.Pubsub =>
        for {
          executorService <- scheduledExecutorService[F](conf)
          consumer <- mkPubsubSource(conf, executorService)
        } yield consumer
      case _ =>
        Resource.eval(Sync[F].raiseError(new IllegalArgumentException(s"Input is not Pubsub")))
    }

  private def mkPubsubSource[F[_]: Async](
    conf: Config.StreamInput.Pubsub,
    executorService: ScheduledExecutorService
  ): Resource[F, Queue.Consumer[F]] =
    Pubsub.consumer[F](
      conf.projectId,
      conf.subscriptionId,
      parallelPullCount = conf.parallelPullCount,
      bufferSize = conf.bufferSize,
      maxAckExtensionPeriod = conf.maxAckExtensionPeriod,
      customPubsubEndpoint = conf.customPubsubEndpoint,
      customizeSubscriber = { s =>
        s.setFlowControlSettings {
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
        }.setExecutorProvider(FixedExecutorProvider.create(executorService))
          .setSystemExecutorProvider(FixedExecutorProvider.create(executorService))
      }
    )

  private def executorResource[F[_]: Sync, E <: ExecutorService](make: F[E]): Resource[F, E] =
    Resource.make(make)(es => Sync[F].blocking(es.shutdown()))

  /**
   * Source operations are backed by two thread pools:
   *
   *   - A single thread pool, which is only even used for maintenance tasks like extending ack
   *     extension periods.
   *   - A small fixed-sized thread pool on which we deliberately run blocking tasks, like
   *     attempting to write to the Queue.
   *
   * Because of this separation, even when the Queue is full, it cannot block the subscriber's
   * maintenance tasks.
   *
   * Note, we use the same exact same thread pools for the subscriber's `executorProvider` and
   * `systemExecutorProvider`. This means when the Queue is full then it blocks the subscriber from
   * fetching more messages from pubsub. We need to do this trick because we have disabled flow
   * control.
   */
  private def scheduledExecutorService[F[_]: Sync](config: Config.StreamInput.Pubsub): Resource[F, ScheduledExecutorService] =
    for {
      forMaintenance <- executorResource(Sync[F].delay(Executors.newSingleThreadScheduledExecutor))
      forBlocking <- executorResource(Sync[F].delay(Executors.newFixedThreadPool(config.parallelPullCount)))
    } yield new ForwardingExecutorService with ScheduledExecutorService {

      /**
       * Any callable/runnable which is **scheduled** must be a maintenance task, so run it on the
       * dedicated maintenance pool
       */
      override def schedule[V](
        callable: Callable[V],
        delay: Long,
        unit: TimeUnit
      ): ScheduledFuture[V] =
        forMaintenance.schedule(callable, delay, unit)
      override def schedule(
        runnable: Runnable,
        delay: Long,
        unit: TimeUnit
      ): ScheduledFuture[_] =
        forMaintenance.schedule(runnable, delay, unit)
      override def scheduleAtFixedRate(
        runnable: Runnable,
        initialDelay: Long,
        period: Long,
        unit: TimeUnit
      ): ScheduledFuture[_] =
        forMaintenance.scheduleAtFixedRate(runnable, initialDelay, period, unit)
      override def scheduleWithFixedDelay(
        runnable: Runnable,
        initialDelay: Long,
        delay: Long,
        unit: TimeUnit
      ): ScheduledFuture[_] =
        forMaintenance.scheduleWithFixedDelay(runnable, initialDelay, delay, unit)

      /**
       * Non-scheduled tasks (e.g. when a message is received), can be run on the fixed-size
       * blocking pool
       */
      override val delegate = forBlocking
    }

  private def mkSink[F[_]: Async](output: Config.Output): Resource[F, BlobStorage[F]] =
    output match {
      case _: Config.Output.GCS =>
        GCS.blobStorage[F]
      case _ =>
        Resource.eval(Async[F].raiseError(new IllegalArgumentException(s"Output is not GCS")))
    }

  private def mkBadQueue[F[_]: Async](output: Config.Output.Bad.Queue): Resource[F, Queue.ChunkProducer[F]] =
    output match {
      case config: Config.Output.Bad.Queue.Pubsub =>
        Pubsub
          .chunkProducer(
            config.projectId,
            config.topicId,
            batchSize = config.batchSize,
            requestByteThreshold = config.requestByteThreshold,
            delayThreshold = config.delayThreshold
          )
      case _ =>
        Resource.eval(Async[F].raiseError(new IllegalArgumentException(s"Message queue is not Pubsub")))
    }

  private def mkQueue[F[_]: Async](queueConfig: Config.QueueConfig): Resource[F, Queue.Producer[F]] =
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
        Resource.eval(Async[F].raiseError(new IllegalArgumentException(s"Message queue is not Pubsub")))
    }
}
