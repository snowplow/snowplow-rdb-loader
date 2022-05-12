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
package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis

import java.util.UUID
import java.net.URI

import scala.concurrent.duration.DurationInt

import cats.implicits._

import cats.effect.concurrent.Ref
import cats.effect.{Blocker, Clock, Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}

import fs2.concurrent.SignallingRef

import blobstore.Store

import io.circe.Json

import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.{InitListCache, InitSchemaCache, Resolver}

import com.snowplowanalytics.aws.AWSQueue

import com.snowplowanalytics.snowplow.rdbloader.common.transformation.EventUtils
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.{MetricsReporters, QueueConfig}

import com.snowplowanalytics.snowplow.rdbloader.transformer.metrics.Metrics
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.{file, s3}

case class Resources[F[_]](
  iglu: Client[F, Json],
  atomicLengths: Map[String, Int],
  awsQueue: AWSQueue[F],
  instanceId: String,
  blocker: Blocker,
  halt: SignallingRef[F, Boolean],
  windows: State.Windows[F],
  global: Ref[F, Long],
  store: Store[F],
  metrics: Metrics[F]
)

object Resources {

  implicit private def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def mk[F[_]: ConcurrentEffect : ContextShift: Clock: InitSchemaCache: InitListCache: Timer](
    igluConfig: Json,
    queueConfig: QueueConfig,
    metricsConfig: MetricsReporters,
    outputPath: URI
  ): Resource[F, Resources[F]] =
    for {
      awsQueue <- mkQueueFromConfig(queueConfig)
      resources <- mk(igluConfig, awsQueue, metricsConfig, outputPath)
    } yield resources

  def mk[F[_]: ConcurrentEffect : ContextShift: Clock: InitSchemaCache: InitListCache: Timer](
    igluConfig: Json,
    awsQueue: AWSQueue[F],
    metricsConfig: MetricsReporters,
    outputPath: URI
  ): Resource[F, Resources[F]] =
    for {
      client        <- mkClient(igluConfig)
      atomicLengths <- mkAtomicFieldLengthLimit(client.resolver)
      instanceId    <- mkTransformerInstanceId
      blocker       <- Blocker[F]
      halt          <- mkHaltingSignal(instanceId)
      initialState  <- mkInitialState
      global        <- Resource.eval(Ref.of(0L))
      store         <- Resource.eval(mkStore[F](blocker, outputPath))
      metrics       <- Resource.eval(Metrics.build[F](blocker, metricsConfig))
    } yield
      Resources(
        client,
        atomicLengths,
        awsQueue,
        instanceId.toString,
        blocker,
        halt,
        initialState,
        global,
        store,
        metrics
      )

  private def mkClient[F[_]: Sync: InitSchemaCache: InitListCache](igluConfig: Json): Resource[F, Client[F, Json]] = Resource.eval {
    Client
      .parseDefault[F](igluConfig)
      .leftMap(e => new RuntimeException(s"Error while parsing Iglu config: ${e.getMessage()}"))
      .value
      .flatMap {
        case Right(init) => Sync[F].pure(init)
        case Left(error) => Sync[F].raiseError[Client[F, Json]](error)
      }
  }

  private def mkAtomicFieldLengthLimit[F[_]: Sync: Clock](igluResolver: Resolver[F]): Resource[F, Map[String, Int]] = Resource.eval {
    EventUtils.getAtomicLengths(igluResolver).flatMap {
      case Right(valid) => Sync[F].pure(valid)
      case Left(error)  => Sync[F].raiseError[Map[String, Int]](error)
    }
  }

  private def mkInitialState[F[_]: Sync] = {
    Resource.make(State.init[F]) { state =>
      state.get.flatMap { stack =>
        if (stack.isEmpty)
          logger.warn(s"Final window state is empty")
        else
          logger.info(s"Final window state:\n${stack.mkString("\n")}")
      }
    }
  }

  private def mkTransformerInstanceId[F[_]: Sync] = {
    Resource
      .eval(Sync[F].delay(UUID.randomUUID()))
      .evalTap(id => logger.info(s"Instantiated $id shredder instance"))
  }

  private def mkHaltingSignal[F[_]: Concurrent: Timer](instanceId: UUID) = {
    Resource.make(SignallingRef(false)) { s =>
      logger.warn("Halting the source, sleeping for 5 seconds...") *>
        s.set(true) *>
        Timer[F].sleep(5.seconds) *>
        logger.warn(s"Shutting down $instanceId instance")
    }
  }

  private def mkQueueFromConfig[F[_]: Concurrent](queueConfig: QueueConfig): Resource[F, AWSQueue[F]] = {
    queueConfig match {
      case QueueConfig.SQS(queueName, region) => AWSQueue.build(AWSQueue.QueueType.SQS, queueName, region.name)
      case QueueConfig.SNS(topicArn, region)  => AWSQueue.build(AWSQueue.QueueType.SNS, topicArn, region.name)
    }
  }

  private def mkStore[F[_]: ConcurrentEffect: ContextShift: Sync](blocker: Blocker, outputPath: URI): F[Store[F]] =
    Option(outputPath.getScheme) match {
      case Some("s3" | "s3a" | "s3n") =>
        s3.initStore[F]
      case Some("file") =>
        Sync[F].delay(file.initStore[F](blocker, outputPath))
      case _ =>
        Sync[F].raiseError(new IllegalArgumentException(s"Can't create store for path $outputPath"))
    }
}
