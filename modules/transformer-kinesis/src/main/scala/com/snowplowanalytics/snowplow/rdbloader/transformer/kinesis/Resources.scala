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

import scala.concurrent.ExecutionContext

import org.typelevel.log4cats.slf4j.Slf4jLogger

import io.circe.Json

import cats.implicits._

import cats.effect.{Blocker, Clock, Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}

import blobstore.Store

import com.snowplowanalytics.lrumap.CreateLruMap

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.{InitListCache, InitSchemaCache, Resolver}
import com.snowplowanalytics.iglu.schemaddl.Properties

import com.snowplowanalytics.aws.AWSQueue

import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{EventUtils, LookupProperties, PropertiesKey}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.QueueConfig

import com.snowplowanalytics.snowplow.rdbloader.transformer.metrics.Metrics
import com.snowplowanalytics.snowplow.rdbloader.transformer.telemetry.Telemetry
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.{file, s3}

import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.generated.BuildInfo

case class Resources[F[_]](
  iglu: Client[F, Json],
  lookup: LookupProperties[F],
  atomicLengths: Map[String, Int],
  awsQueue: AWSQueue[F],
  instanceId: String,
  blocker: Blocker,
  store: Store[F],
  metrics: Metrics[F],
  telemetry: Telemetry[F]
)

object Resources {

  implicit private def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def mk[F[_]: ConcurrentEffect : ContextShift: Clock: InitSchemaCache: InitListCache: Timer](
    igluConfig: Json,
    config: Config,
    executionContext: ExecutionContext
  ): Resource[F, Resources[F]] =
    for {
      awsQueue <- mkQueueFromConfig(config.queue)
      resources <- mk(igluConfig, config, awsQueue, executionContext)
    } yield resources

  def mk[F[_]: ConcurrentEffect : ContextShift: Clock: InitSchemaCache: InitListCache: Timer](
    igluConfig: Json,
    config: Config,
    awsQueue: AWSQueue[F],
    executionContext: ExecutionContext
  ): Resource[F, Resources[F]] =
    for {
      client        <- mkClient(igluConfig)
      lookup        <- Resource.eval(CreateLruMap[F, PropertiesKey, Properties].create(100))
      atomicLengths <- mkAtomicFieldLengthLimit(client.resolver)
      instanceId    <- mkTransformerInstanceId
      blocker       <- Blocker[F]
      store         <- Resource.eval(mkStore[F](blocker, config.output.path))
      metrics       <- Resource.eval(Metrics.build[F](blocker, config.monitoring.metrics))
      telemetry     <- Telemetry.build[F](config, BuildInfo.name, BuildInfo.version, executionContext)
    } yield
      Resources(
        client,
        lookup,
        atomicLengths,
        awsQueue,
        instanceId.toString,
        blocker,
        store,
        metrics,
        telemetry
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

  private def mkTransformerInstanceId[F[_]: Sync] = {
    Resource
      .eval(Sync[F].delay(UUID.randomUUID()))
      .evalTap(id => logger.info(s"Instantiated $id shredder instance"))
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
