/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader.discovery

import cats._
import cats.data.EitherT
import cats.implicits._
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.core.SchemaKey.ordering
import com.snowplowanalytics.iglu.schemaddl.redshift.{
  MergeRedshiftSchemasResult,
  ShredModel,
  foldMapMergeRedshiftSchemas,
  foldMapRedshiftSchemas
}
import com.snowplowanalytics.snowplow.rdbloader.cloud.JsonPathDiscovery
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import com.snowplowanalytics.snowplow.rdbloader.{LoaderAction, LoaderError}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Cache, Iglu, Logging}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow.WideRowFormat
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery.DiscoveredShredModels
import com.snowplowanalytics.snowplow.rdbloader.discovery.DiscoveryFailure.IgluError
import scala.math.Ordered.orderingToOrdered

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
  wideColumns: List[String],
  shredModels: Map[SchemaKey, DiscoveredShredModels]
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

  /**
   * @param shredModel
   *   Used to construct column names for COPY FROM query
   * @param mergeRedshiftSchemasResult
   *   Used for migration
   */
  case class DiscoveredShredModels(
    shredModel: ShredModel,
    mergeRedshiftSchemasResult: MergeRedshiftSchemasResult
  )

  case class WithOrigin(
    discovery: DataDiscovery,
    origin: LoaderMessage.ShreddingComplete
  )

  /** The result of classifying one raw queue message, before anything has been acked or loaded. */
  sealed trait ClassifiedMessage
  object ClassifiedMessage {

    /**
     * There is nothing loadable here: either the folder is empty, or the envelope couldn't be
     * parsed and never will be. Both are final -- ack it, it will never need retrying.
     */
    case object NothingToLoad extends ClassifiedMessage

    /**
     * A post-parse discovery failure (e.g. an Iglu registry blip, a missing JSONPath). Unlike an
     * unparseable envelope this can be transient, so the caller retries before giving up.
     */
    case class Failed(base: BlobStorage.Folder, error: LoaderError) extends ClassifiedMessage
    case class Discovered(withOrigin: WithOrigin) extends ClassifiedMessage
  }

  /**
   * Parse a raw queue message and classify it. Pure classification: it neither acks nor extends the
   * message, so it carries no opinion on when either should happen -- that's `Loader.loadStream`'s
   * call.
   *
   * @param assets
   *   optional bucket with custom JSONPaths
   * @param content
   *   the raw queue message body
   */
  def classify[F[_]: MonadThrow: BlobStorage: Cache: Logging: JsonPathDiscovery: Iglu](
    assets: Option[BlobStorage.Folder],
    content: String
  ): F[ClassifiedMessage] =
    LoaderMessage.fromString(content) match {
      case Left(error) =>
        val loaderError = DiscoveryFailure.IgluError(error).toLoaderError
        Logging[F]
          .error(loaderError)("A problem occurred in the loading of the queue message. Acknowledging and skipping it.")
          .as(ClassifiedMessage.NothingToLoad)
      // Checked ahead of discovery, which an empty folder has nothing to look up for anyway
      case Right(parsed) if isEmpty(parsed.message) =>
        Logging[F]
          .info(s"Empty discovery at ${parsed.message.base}. Acknowledging the message without loading attempt")
          .as(ClassifiedMessage.NothingToLoad)
      case Right(parsed) =>
        fromLoaderMessage[F](assets, parsed).value.flatMap {
          case Right(discovery) =>
            Logging[F]
              .info(s"New data discovery at ${discovery.show}")
              .as(ClassifiedMessage.Discovered(WithOrigin(discovery, parsed.message)))
          case Left(error) =>
            Logging[F]
              .error(error)("A problem occurred in the loading of the queue message")
              .as(ClassifiedMessage.Failed(parsed.message.base, error))
        }
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
  def fromLoaderMessage[F[_]: Monad: Cache: BlobStorage: JsonPathDiscovery: Iglu: Logging](
    assets: Option[BlobStorage.Folder],
    parsed: LoaderMessage.Parsed
  ): LoaderAction[F, DataDiscovery] = {
    val message = parsed.message
    val types = ShreddedType
      .fromCommon[F](message.base, assets, message.typesInfo)
      .map { steps =>
        LoaderError.DiscoveryError.fromValidated(steps.traverse(_.toValidatedNel))
      }

    for {
      types <- LoaderAction[F, List[ShreddedType]](types)
      nonAtomicTypes = types.distinct.filterNot(_.isAtomic)
      wideColumns <- message.typesInfo match {
                       case TypesInfo.Shredded(_) => EitherT.pure[F, LoaderError](List.empty[String])
                       case TypesInfo.WideRow(fileFormat, types) =>
                         fileFormat match {
                           case WideRowFormat.JSON => EitherT.pure[F, LoaderError](List.empty[String])
                           case WideRowFormat.PARQUET =>
                             parquetColumns[F](message.base, types, parsed.columnNamesAreComplete)
                         }
                     }
      models <- message.typesInfo match {
                  case TypesInfo.Shredded(_)   => getShredModels[F](nonAtomicTypes)
                  case TypesInfo.WideRow(_, _) => EitherT.rightT[F, LoaderError](Map.empty[SchemaKey, DiscoveredShredModels])
                }
    } yield DataDiscovery(message.base, types.distinct, message.compression, message.typesInfo, wideColumns, models)
  }

  /**
   * The parquet columns to copy.
   *
   * When the message's version promises the column names are complete (see
   * [[LoaderMessage.Parsed]]) they are used exactly as given, and a type with no name is skipped:
   * its column holds no data for this batch, so there is nothing to copy. Deriving the names here
   * is a fallback for older messages only -- a recovery column's name embeds a hash that differs
   * between Scala versions, so a 2.13 transformer and this 2.12 loader would not agree on it.
   */
  /** Column names from the message are interpolated into `COPY INTO ... SELECT` without quoting. */
  private val ValidColumnName = "^[A-Za-z_][A-Za-z0-9_]*$".r

  private def parquetColumns[F[_]: Monad: Iglu: Logging](
    base: BlobStorage.Folder,
    types: List[TypesInfo.WideRow.Type],
    columnNamesAreComplete: Boolean
  ): EitherT[F, LoaderError, List[String]] =
    if (columnNamesAreComplete) {
      val names = types.flatMap(_.columnName).distinct
      names.find(name => ValidColumnName.findFirstIn(name).isEmpty) match {
        case Some(invalid) =>
          EitherT.leftT[F, List[String]](
            LoaderError.DiscoveryError(IgluError(show"Message for $base names an invalid parquet column [$invalid]")): LoaderError
          )
        case None => EitherT.pure[F, LoaderError](names)
      }
    } else
      EitherT.liftF[F, LoaderError, Unit] {
        Logging[F].info(show"Message for $base predates parquet column names; deriving them locally")
      } *> Iglu[F]
        .fieldNamesFromTypes(types)
        .leftMap(er => LoaderError.DiscoveryError(IgluError(s"Error inferring columns names $er")))

  def getShredModels[F[_]: Monad: Iglu](
    nonAtomicTypes: List[ShreddedType]
  ): EitherT[F, LoaderError, Map[SchemaKey, DiscoveredShredModels]] = {
    val maxSchemaKeyPerTableName = getMaxSchemaKeyPerTableName(nonAtomicTypes)
    nonAtomicTypes
      .traverse { shreddedType =>
        EitherT(Iglu[F].getSchemasWithSameModel(shreddedType.info.getSchemaKey)).map { schemas =>
          val maxSchemaKey                             = maxSchemaKeyPerTableName(shreddedType.info.getNameFull)
          val filtered                                 = schemas.filter(_.self.schemaKey <= shreddedType.info.getSchemaKey).toNel.get
          val maxFiltered                              = schemas.filter(_.self.schemaKey <= maxSchemaKey).toNel.get
          val foldMapRedshiftSchemasResult: ShredModel = foldMapRedshiftSchemas(filtered)(shreddedType.info.getSchemaKey)
          val foldMapMergeRedshiftSchemasResult        = foldMapMergeRedshiftSchemas(maxFiltered)
          (shreddedType.info.getSchemaKey, DiscoveredShredModels(foldMapRedshiftSchemasResult, foldMapMergeRedshiftSchemasResult))
        }
      }
      .map(_.toMap)
  }

  implicit val ord: Ordering[SchemaKey] = ordering

  /** Find the maximum SchemaKey for all table names in a given set of shredded types */
  def getMaxSchemaKeyPerTableName(shreddedTypes: List[ShreddedType]): Map[String, SchemaKey] =
    shreddedTypes.groupBy(_.info.getNameFull).mapValues(_.maxBy(_.info.version).info.getSchemaKey)

  /** Check if discovery contains no data */
  def isEmpty(message: LoaderMessage.ShreddingComplete): Boolean =
    message.timestamps.min.isEmpty && message.timestamps.max.isEmpty && message.typesInfo.isEmpty && message.count.forall(_.good == 0)
}
