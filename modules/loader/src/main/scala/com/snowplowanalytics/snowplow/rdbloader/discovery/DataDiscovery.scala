/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.discovery

import cats._
import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.{DiscoveryStep, DiscoveryStream, LoaderError, LoaderAction, State}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, AWS, Cache}
import com.snowplowanalytics.snowplow.rdbloader.common.{S3, Message, LoaderMessage}
import com.snowplowanalytics.snowplow.rdbloader.common.config.Config
import com.snowplowanalytics.snowplow.rdbloader.common.config.Config.Shredder.Compression

/**
  * Result of data discovery in shredded.good folder
  * It still exists mostly to fulfill legacy batch-discovery behavior,
  * once Loader entirely switched to RT architecture we can replace it with
  * `ShreddingComplete` message
  * @param base shred run folder full path
  * @param shreddedTypes list of shredded types in this directory
  */
case class DataDiscovery(base: S3.Folder, shreddedTypes: List[ShreddedType], compression: Compression) {
  /** ETL id */
  def runId: String = base.split("/").last

  def show: String = {
    val shreddedTypesList = shreddedTypes.map(x => s"  * ${x.show}").mkString("\n")
    val shredded = if (shreddedTypes.isEmpty) "without shredded types" else s"with following shredded types:\n$shreddedTypesList"
    s"$runId $shredded"
  }
}

/**
 * This module provides data-discovery mechanism for "atomic" events only
 * and for "full" discovery (atomic and shredded types)
 * Primary methods return lists of `DataDiscovery` results, where
 * each `DataDiscovery` represents particular `run=*` folder in shredded.good
 *
 * It lists all keys in run id folder, parse each one as atomic-events key or
 * shredded type key and then groups by run ids
 */
object DataDiscovery {

  private implicit val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  case class WithOrigin(discovery: DataDiscovery, origin: LoaderMessage.ShreddingComplete)

  /**
   * App entrypoint, a generic discovery stream reading from message queue (like SQS)
   * The plain text queue will be parsed into known message types (`LoaderMessage`) and
   * handled appropriately - transformed either into action or into [[DataDiscovery]]
   * In case of any error (parsing or transformation) the error will be raised
   * Empty folder will be reported, but stream will keep running
   *
   * The stream is responsible for state changing as well
   *
   * @param config generic storage target configuration
   * @param state mutable state to keep logging information
   */
  def discover[F[_]: MonadThrow: AWS: Cache: Logging](config: Config[_], state: State.Ref[F]): DiscoveryStream[F] =
    AWS[F]
      .readSqs(config.messageQueue)
      .evalMapFilter { message =>
        val action = LoaderMessage.fromString(message.data) match {
          case Right(parsed: LoaderMessage.ShreddingComplete) =>
            handle(config.region, config.jsonpaths, parsed, message.ack)
          case Left(error) =>
            ackAndRaise[F](DiscoveryFailure.IgluError(error).toLoaderError, message.ack)
        }

        Logging[F].info("Received a new message") *>
          Logging[F].debug(message.data) *>
          state.updateAndGet(_.incrementMessages).flatMap(state => Logging[F].info(state.show)) *> 
          action
      }

  /**
   * Get `DataDiscovery` or log an error and drop the message
   * actionable `DataDiscovery`
   * @param region AWS region to discover JSONPaths
   * @param assets optional bucket with custom JSONPaths
   * @param message payload coming from shredder
   * @tparam F effect type to perform AWS interactions
   */
  def handle[F[_]: MonadThrow: AWS: Cache: Logging](region: String,
                                                    assets: Option[S3.Folder],
                                                    message: LoaderMessage.ShreddingComplete,
                                                    ack: F[Unit]) =
    fromLoaderMessage[F](region, assets, message)
      .value
      .flatMap[Option[Message[F, WithOrigin]]] {
        case Right(_) if isEmpty(message) =>
          Logging[F].info(s"Empty discovery at ${message.base}. Acknowledging the message without loading attempt") *>
            ack.as(none[Message[F, WithOrigin]])
        case Right(discovery) =>
          Logging[F]
            .info(s"New data discovery at ${discovery.show}")
            .as(Some(Message(WithOrigin(discovery, message), ack)))
        case Left(error) =>
          ackAndRaise[F](error, ack)
      }

  /**
   * Convert `ShreddingComplete` message coming from shredder into
   * actionable `DataDiscovery`
   * @param region AWS region to discover JSONPaths
   * @param assets optional bucket with custo mJSONPaths
   * @param message payload coming from shredder
   * @tparam F effect type to perform AWS interactions
   */
  def fromLoaderMessage[F[_]: Monad: Cache: AWS](region: String,
                                                 assets: Option[S3.Folder],
                                                 message: LoaderMessage.ShreddingComplete): LoaderAction[F, DataDiscovery] = {
    val types = message
      .types
      .traverse[F, DiscoveryStep[ShreddedType]] { shreddedType =>
        ShreddedType.fromCommon[F](message.base, message.processor.version, region, assets, shreddedType)
      }
      .map { steps => LoaderError.DiscoveryError.fromValidated(steps.traverse(_.toValidatedNel)) }
    LoaderAction[F, List[ShreddedType]](types).map { types =>
      DataDiscovery(message.base, types.distinct, message.compression)
    }
  }

  def ackAndRaise[F[_]: MonadThrow: Logging](error: LoaderError, ack: F[Unit]): F[Option[Message[F, WithOrigin]]] =
    Logging[F].error(error)("A problem occured in the loading of SQS message") *> ack *> MonadThrow[F].raiseError(error)

  /** Check if discovery contains no data */
  def isEmpty(message: LoaderMessage.ShreddingComplete): Boolean =
    message.timestamps.min.isEmpty && message.timestamps.max.isEmpty && message.types.isEmpty && message.count.contains(0)
}
