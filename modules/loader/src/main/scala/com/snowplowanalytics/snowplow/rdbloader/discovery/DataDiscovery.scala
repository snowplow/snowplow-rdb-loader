/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.discovery

import cats._
import cats.data.EitherT
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.cloud.JsonPathDiscovery
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import com.snowplowanalytics.snowplow.rdbloader.{DiscoveryStream, LoaderAction, LoaderError}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Cache, Iglu, Logging}
import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow.WideRowFormat
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.discovery.DiscoveryFailure.IgluError
import com.snowplowanalytics.snowplow.rdbloader.state.State

/**
 * Result of data discovery in transformer output folder It still exists mostly to fulfill legacy
 * batch-discovery behavior, once Loader entirely switched to RT architecture we can replace it with
 * `ShreddingComplete` message
 * @param base
 *   transformed run folder full path
 * @param shreddedTypes
 *   list of shredded types in this directory
 */
case class DataDiscovery(
  base: BlobStorage.Folder,
  shreddedTypes: List[ShreddedType],
  compression: Compression,
  typesInfo: TypesInfo,
  columns: List[String]
) {

  /** ETL id */
  def runId: String = base.split("/").last

  def show: String = {
    val typeList = shreddedTypes.map(x => s"  * ${x.show}").mkString("\n")
    val shredded = if (shreddedTypes.isEmpty) "without shredded types" else s"with following shredded types:\n$typeList"
    s"$runId $shredded"
  }
}

/**
 * This module provides data-discovery mechanism for "atomic" events only and for "full" discovery
 * (atomic and shredded types) Primary methods return lists of `DataDiscovery` results, where each
 * `DataDiscovery` represents particular `run=*` folder in shredded.good
 *
 * It lists all keys in run id folder, parse each one as atomic-events key or shredded type key and
 * then groups by run ids
 */
object DataDiscovery {

  private implicit val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  case class WithOrigin(discovery: DataDiscovery, origin: LoaderMessage.ShreddingComplete)

  /**
   * App entrypoint, a generic discovery stream reading from message queue (like SQS) The plain text
   * queue will be parsed into known message types (`LoaderMessage`) and handled appropriately -
   * transformed either into action or into [[DataDiscovery]] In case of any error (parsing or
   * transformation) the error will be raised Empty folder will be reported, but stream will keep
   * running
   *
   * The stream is responsible for state changing as well
   *
   * @param config
   *   generic storage target configuration
   */
  def discover[F[_]: MonadThrow: BlobStorage: Iglu: Cache: Queue.Consumer: Logging: JsonPathDiscovery](
    config: Config[_],
    incrementMessages: F[State]
  ): DiscoveryStream[F] =
    Queue
      .Consumer[F]
      .read
      .evalMapFilter { message =>
        val action = LoaderMessage.fromString(message.content) match {
          case Right(parsed: LoaderMessage.ShreddingComplete) =>
            handle[F](config.jsonpaths, parsed)
          case Left(error) =>
            logAndRaise[F](DiscoveryFailure.IgluError(error).toLoaderError)
        }

        Logging[F].info("Received a new message") *>
          Logging[F].debug(message.content) *>
          incrementMessages.flatMap(state => Logging[F].info(state.show)) *>
          action
      }

  /**
   * Get `DataDiscovery` or log an error and drop the message actionable `DataDiscovery`
   * @param assets
   *   optional bucket with custom JSONPaths
   * @param message
   *   payload coming from transformer
   * @tparam F
   *   effect type to perform AWS interactions
   */
  def handle[F[_]: MonadThrow: BlobStorage: Cache: Logging: JsonPathDiscovery: Iglu](
    assets: Option[BlobStorage.Folder],
    message: LoaderMessage.ShreddingComplete
  ): F[Option[WithOrigin]] =
    fromLoaderMessage[F](assets, message).value
      .flatMap[Option[WithOrigin]] {
        case Right(_) if isEmpty(message) =>
          Logging[F].info(s"Empty discovery at ${message.base}. Acknowledging the message without loading attempt").as(none)
        case Right(discovery) =>
          Logging[F]
            .info(s"New data discovery at ${discovery.show}")
            .as(Some(WithOrigin(discovery, message)))
        case Left(error) =>
          logAndRaise[F](error)
      }

  /**
   * Convert `ShreddingComplete` message coming from shredder into actionable `DataDiscovery`
   * @param assets
   *   optional bucket with custo mJSONPaths
   * @param message
   *   payload coming from shredder
   * @tparam F
   *   effect type to perform AWS interactions
   */
  def fromLoaderMessage[F[_]: Monad: Cache: BlobStorage: JsonPathDiscovery: Iglu](
    assets: Option[BlobStorage.Folder],
    message: LoaderMessage.ShreddingComplete
  ): LoaderAction[F, DataDiscovery] = {
    val types = ShreddedType
      .fromCommon[F](message.base, assets, message.typesInfo)
      .map { steps =>
        LoaderError.DiscoveryError.fromValidated(steps.traverse(_.toValidatedNel))
      }

    for {
      types <- LoaderAction[F, List[ShreddedType]](types)
      columns <- message.typesInfo match {
                   case TypesInfo.Shredded(_) => EitherT.pure[F, LoaderError](List.empty[String])
                   case TypesInfo.WideRow(fileFormat, types) =>
                     fileFormat match {
                       case WideRowFormat.JSON => EitherT.pure[F, LoaderError](List.empty[String])
                       case WideRowFormat.PARQUET =>
                         Iglu[F]
                           .fieldNamesFromTypes(types)
                           .leftMap(er => LoaderError.DiscoveryError(IgluError(s"Error inferring columns names $er")))
                     }
                 }
    } yield DataDiscovery(message.base, types.distinct, message.compression, message.typesInfo, columns)
  }

  def logAndRaise[F[_]: MonadThrow: Logging](error: LoaderError): F[Option[WithOrigin]] =
    Logging[F].error(error)("A problem occurred in the loading of SQS message") *> MonadThrow[F].raiseError(error)

  /** Check if discovery contains no data */
  def isEmpty(message: LoaderMessage.ShreddingComplete): Boolean =
    message.timestamps.min.isEmpty && message.timestamps.max.isEmpty && message.typesInfo.isEmpty && message.count.forall(_.good == 0)
}
