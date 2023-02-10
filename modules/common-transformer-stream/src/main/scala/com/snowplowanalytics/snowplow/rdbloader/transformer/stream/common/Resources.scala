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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import java.util.UUID

import scala.concurrent.ExecutionContext

import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.Logger

import io.circe.Json

import cats.Applicative
import cats.implicits._
import cats.effect._

import org.http4s.client.blaze.BlazeClientBuilder

import io.sentry.SentryClient

import com.snowplowanalytics.iglu.client.resolver.{InitListCache, InitSchemaCache, Resolver}
import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverConfig
import com.snowplowanalytics.iglu.schemaddl.Properties
import com.snowplowanalytics.lrumap.CreateLruMap
import com.snowplowanalytics.snowplow.rdbloader.common.Sentry
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.common.telemetry.Telemetry
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{EventUtils, PropertiesCache, PropertiesKey}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Config.Output.Bad
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.metrics.Metrics
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks.BadSink
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sources.Checkpointer

case class Resources[F[_], C](
  igluResolver: Resolver[F],
  propertiesCache: PropertiesCache[F],
  atomicLengths: Map[String, Int],
  producer: Queue.Producer[F],
  instanceId: String,
  blocker: Blocker,
  metrics: Metrics[F],
  telemetry: Telemetry[F],
  sentry: Option[SentryClient],
  inputStream: Queue.Consumer[F],
  checkpointer: Queue.Consumer.Message[F] => C,
  blobStorage: BlobStorage[F],
  badSink: BadSink[F]
)

object Resources {

  implicit private def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def mk[F[_]: ConcurrentEffect: ContextShift: Clock: InitSchemaCache: InitListCache: Timer, C: Checkpointer[F, *]](
    igluConfig: Json,
    config: Config,
    buildName: String,
    buildVersion: String,
    executionContext: ExecutionContext,
    mkSource: (Blocker, Config.StreamInput, Config.Monitoring) => Resource[F, Queue.Consumer[F]],
    mkSink: (Blocker, Config.Output) => Resource[F, BlobStorage[F]],
    mkBadQueue: (Blocker, Config.Output.Bad.Queue) => Resource[F, Queue.ChunkProducer[F]],
    mkQueue: Config.QueueConfig => Resource[F, Queue.Producer[F]],
    checkpointer: Queue.Consumer.Message[F] => C
  ): Resource[F, Resources[F, C]] =
    for {
      producer <- mkQueue(config.queue)
      resolverConfig <- mkResolverConfig(igluConfig)
      resolver <- mkResolver(resolverConfig)
      propertiesCache <- Resource.eval(CreateLruMap[F, PropertiesKey, Properties].create(resolverConfig.cacheSize))
      atomicLengths <- mkAtomicFieldLengthLimit(resolver)
      instanceId <- mkTransformerInstanceId
      blocker <- Blocker[F]
      metrics <- Resource.eval(Metrics.build[F](blocker, config.monitoring.metrics))
      sentry <- Sentry.init[F](config.monitoring.sentry.map(_.dsn))
      httpClient <- BlazeClientBuilder[F](executionContext).resource
      telemetry <- Telemetry.build[F](
                     config.telemetry,
                     buildName,
                     buildVersion,
                     httpClient,
                     AppId.appId,
                     getRegionFromConfig(config),
                     getCloudFromConfig(config)
                   )
      inputStream <- mkSource(blocker, config.input, config.monitoring)
      blobStorage <- mkSink(blocker, config.output)
      badSink <- mkBadSink(config, mkBadQueue, blocker)
    } yield Resources(
      resolver,
      propertiesCache,
      atomicLengths,
      producer,
      instanceId.toString,
      blocker,
      metrics,
      telemetry,
      sentry,
      inputStream,
      checkpointer,
      blobStorage,
      badSink
    )

  private def mkBadSink[F[_]: Applicative](
    config: Config,
    mkBadQueue: (Blocker, Bad.Queue) => Resource[F, Queue.ChunkProducer[F]],
    blocker: Blocker
  ): Resource[F, BadSink[F]] =
    config.output.bad match {
      case Bad.File => Resource.pure[F, BadSink[F]](BadSink.UseBlobStorage())
      case queueConfig: Bad.Queue => mkBadQueue(blocker, queueConfig).map(BadSink.UseQueue(_))
    }

  private def mkResolverConfig[F[_]: Sync](igluConfig: Json): Resource[F, ResolverConfig] = Resource.eval {
    Resolver.parseConfig(igluConfig) match {
      case Right(resolverConfig) => Sync[F].pure(resolverConfig)
      case Left(error) =>
        Sync[F].raiseError[ResolverConfig](new RuntimeException(s"Error while parsing Iglu resolver config: ${error.getMessage()}"))
    }
  }

  private def mkResolver[F[_]: Sync: InitSchemaCache: InitListCache](resolverConfig: ResolverConfig): Resource[F, Resolver[F]] =
    Resource.eval {
      Resolver
        .fromConfig[F](resolverConfig)
        .leftMap(e => new RuntimeException(s"Error while parsing Iglu resolver config: ${e.getMessage()}"))
        .value
        .flatMap {
          case Right(init) => Sync[F].pure(init)
          case Left(error) => Sync[F].raiseError[Resolver[F]](error)
        }
    }

  private def mkAtomicFieldLengthLimit[F[_]: Sync: Clock](igluResolver: Resolver[F]): Resource[F, Map[String, Int]] = Resource.eval {
    EventUtils.getAtomicLengths(igluResolver).flatMap {
      case Right(valid) => Sync[F].pure(valid)
      case Left(error) => Sync[F].raiseError[Map[String, Int]](error)
    }
  }

  private def mkTransformerInstanceId[F[_]: Sync] =
    Resource
      .eval(Sync[F].delay(UUID.randomUUID()))
      .evalTap(id => logger.info(s"Instantiated $id shredder instance"))

  private def getRegionFromConfig(config: Config): Option[String] =
    config.input match {
      case c: Config.StreamInput.Kinesis => Some(c.region.name)
      case _ => None
    }

  private def getCloudFromConfig(config: Config): Option[Telemetry.Cloud] =
    config.input match {
      case _: Config.StreamInput.Kinesis => Some(Telemetry.Cloud.Aws)
      case _: Config.StreamInput.Pubsub => Some(Telemetry.Cloud.Gcp)
      case _ => None
    }
}
