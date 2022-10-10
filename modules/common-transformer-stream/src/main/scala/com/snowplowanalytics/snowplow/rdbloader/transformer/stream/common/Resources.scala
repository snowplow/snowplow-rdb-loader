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

import cats.implicits._
import cats.effect._

import org.http4s.client.blaze.BlazeClientBuilder

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.{InitListCache, InitSchemaCache, Resolver}

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{Queue, BlobStorage}
import com.snowplowanalytics.snowplow.rdbloader.common.telemetry.Telemetry
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.EventUtils

import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.metrics.Metrics
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sources.Checkpointer

case class Resources[F[_], C](
  iglu: Client[F, Json],
  atomicLengths: Map[String, Int],
  producer: Queue.Producer[F],
  instanceId: String,
  blocker: Blocker,
  metrics: Metrics[F],
  telemetry: Telemetry[F],
  inputStream: Queue.Consumer[F],
  checkpointer: Queue.Consumer.Message[F] => C,
  blobStorage: BlobStorage[F]
) {
  implicit val implBlobStorage: BlobStorage[F] = blobStorage
}

object Resources {

  implicit private def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def mk[F[_]: ConcurrentEffect : ContextShift: Clock: InitSchemaCache: InitListCache: Timer,
         C: Checkpointer[F, *]](
    igluConfig: Json,
    config: Config,
    buildName: String,
    buildVersion: String,
    executionContext: ExecutionContext,
    mkSource: (Blocker, Config.StreamInput, Config.Monitoring) => Resource[F, Queue.Consumer[F]],
    mkSink: (Blocker, Config.Output) => Resource[F, BlobStorage[F]],
    mkQueue: Config.QueueConfig => Resource[F, Queue.Producer[F]],
    checkpointer: Queue.Consumer.Message[F] => C
  ): Resource[F, Resources[F, C]] =
    for {
      producer      <- mkQueue(config.queue)
      client        <- mkClient(igluConfig)
      atomicLengths <- mkAtomicFieldLengthLimit(client.resolver)
      instanceId    <- mkTransformerInstanceId
      blocker       <- Blocker[F]
      metrics       <- Resource.eval(Metrics.build[F](blocker, config.monitoring.metrics))
      httpClient    <- BlazeClientBuilder[F](executionContext).resource
      telemetry     <- Telemetry.build[F](
        config.telemetry,
        buildName,
        buildVersion,
        httpClient,
        AppId.appId,
        getRegionFromConfig(config),
        getCloudFromConfig(config)
      )
      inputStream   <- mkSource(blocker, config.input, config.monitoring)
      blobStorage   <- mkSink(blocker, config.output)
    } yield
      Resources(
        client,
        atomicLengths,
        producer,
        instanceId.toString,
        blocker,
        metrics,
        telemetry,
        inputStream,
        checkpointer,
        blobStorage
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
