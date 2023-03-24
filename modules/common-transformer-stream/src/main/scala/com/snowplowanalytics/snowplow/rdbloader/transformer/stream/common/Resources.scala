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
import cats.effect.std.Random

import io.sentry.SentryClient
import com.snowplowanalytics.iglu.client.resolver.{CreateResolverCache, Resolver}
import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverConfig
import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, RegistryLookup}
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModel
import com.snowplowanalytics.lrumap.CreateLruMap
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.rdbloader.common.Sentry
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.common.telemetry.Telemetry
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.EventUtils.EventParser
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{EventUtils, ShredModelCache}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Config.Output.Bad
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.metrics.Metrics
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks.BadSink
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sources.Checkpointer

import com.snowplowanalytics.snowplow.scalatracker.Tracking

import org.http4s.blaze.client.BlazeClientBuilder

case class Resources[F[_], C](
  igluResolver: Resolver[F],
  shredModelCache: ShredModelCache[F],
  eventParser: EventParser,
  producer: Queue.Producer[F],
  instanceId: String,
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

  def mk[F[_]: Async: Tracking, C: Checkpointer[F, *]](
    igluConfig: Json,
    config: Config,
    buildName: String,
    buildVersion: String,
    executionContext: ExecutionContext,
    mkSource: (Config.StreamInput, Config.Monitoring) => Resource[F, Queue.Consumer[F]],
    mkSink: Config.Output => Resource[F, BlobStorage[F]],
    mkBadQueue: Config.Output.Bad.Queue => Resource[F, Queue.ChunkProducer[F]],
    mkQueue: Config.QueueConfig => Resource[F, Queue.Producer[F]],
    checkpointer: Queue.Consumer.Message[F] => C
  ): Resource[F, Resources[F, C]] =
    for {
      producer <- mkQueue(config.queue)
      resolverConfig <- mkResolverConfig(igluConfig)
      resolver <- mkResolver(resolverConfig)
      shredModelCache <- Resource.eval(CreateLruMap[F, SchemaKey, ShredModel].create(resolverConfig.cacheSize))
      httpClient <- BlazeClientBuilder[F].withExecutionContext(executionContext).resource
      implicit0(registryLookup: RegistryLookup[F]) <- Resource.pure(Http4sRegistryLookup[F](httpClient))
      eventParser <- mkEventParser(resolver, config)
      instanceId <- mkTransformerInstanceId
      metrics <- Resource.eval(Metrics.build[F](config.monitoring.metrics))
      sentry <- Sentry.init[F](config.monitoring.sentry.map(_.dsn))
      implicit0(random: Random[F]) <- Resource.eval(Random.scalaUtilRandom[F])
      telemetry <- Telemetry.build[F](
                     config.telemetry,
                     buildName,
                     buildVersion,
                     httpClient,
                     AppId.appId,
                     getRegionFromConfig(config),
                     getCloudFromConfig(config)
                   )
      inputStream <- mkSource(config.input, config.monitoring)
      blobStorage <- mkSink(config.output)
      badSink <- mkBadSink(config, mkBadQueue)
    } yield Resources(
      resolver,
      shredModelCache,
      eventParser,
      producer,
      instanceId.toString,
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
    mkBadQueue: Bad.Queue => Resource[F, Queue.ChunkProducer[F]]
  ): Resource[F, BadSink[F]] =
    config.output.bad match {
      case Bad.File => Resource.pure[F, BadSink[F]](BadSink.UseBlobStorage())
      case queueConfig: Bad.Queue => mkBadQueue(queueConfig).map(BadSink.UseQueue(_))
    }

  private def mkResolverConfig[F[_]: Sync](igluConfig: Json): Resource[F, ResolverConfig] = Resource.eval {
    Resolver.parseConfig(igluConfig) match {
      case Right(resolverConfig) => Sync[F].pure(resolverConfig)
      case Left(error) =>
        Sync[F].raiseError[ResolverConfig](new RuntimeException(s"Error while parsing Iglu resolver config: ${error.getMessage()}"))
    }
  }

  private def mkResolver[F[_]: Sync: CreateResolverCache](resolverConfig: ResolverConfig): Resource[F, Resolver[F]] =
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
  private def mkEventParser[F[_]: Sync: RegistryLookup: Clock](igluResolver: Resolver[F], config: Config): Resource[F, EventParser] =
    Resource.eval {
      mkAtomicLengths(igluResolver, config).flatMap {
        case Right(atomicLengths) => Sync[F].pure(Event.parser(atomicLengths))
        case Left(error) => Sync[F].raiseError[EventParser](error)
      }
    }
  private def mkAtomicLengths[F[_]: Sync: RegistryLookup: Clock](
    igluResolver: Resolver[F],
    config: Config
  ): F[Either[RuntimeException, Map[String, Int]]] =
    if (config.featureFlags.truncateAtomicFields) {
      EventUtils.getAtomicLengths(igluResolver)
    } else {
      Sync[F].pure(Right(Map.empty[String, Int]))
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
