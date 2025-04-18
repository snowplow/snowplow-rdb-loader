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

import cats.Monad
import cats.implicits._
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.rdbloader.DiscoveryStep
import com.snowplowanalytics.snowplow.rdbloader.cloud.JsonPathDiscovery
import com.snowplowanalytics.snowplow.rdbloader.common.{Common, LoaderMessage}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import com.snowplowanalytics.snowplow.rdbloader.dsl.Cache
import com.snowplowanalytics.snowplow.rdbloader.common.Common.toSnakeCase
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage

/**
 * Generally same as `LoaderMessage.ShreddedType`, but for JSON types holds an information about
 * discovered JSONPath file and does NOT contain full SchemaVer
 *
 * Can be converted from `LoaderMessage.ShreddedType` using `DataDiscover.fromLoaderMessage`
 */
sealed trait ShreddedType {

  /** raw metadata extracted from S3 Key */
  def info: ShreddedType.Info

  /** Get S3 prefix which DB should COPY FROM */
  def getLoadPath: String

  /** Human-readable form */
  def show: String

  /** Check if this type is special atomic type */
  def isAtomic = this match {
    case ShreddedType.Tabular(ShreddedType.Info(_, vendor, name, v, _)) =>
      vendor == Common.AtomicSchema.vendor && name == Common.AtomicSchema.name && v.model == Common.AtomicSchema.version.model && v.revision == Common.AtomicSchema.version.revision && v.addition == Common.AtomicSchema.version.addition
    case _ =>
      false
  }

  def getSnowplowEntity: LoaderMessage.SnowplowEntity = info.entity
}

/**
 * Companion object for `ShreddedType` containing discovering functions
 */
object ShreddedType {

  /**
   * Container for S3 folder with shredded JSONs ready to load with JSONPaths Usually it represents
   * self-describing event or custom/derived context
   *
   * @param jsonPaths
   *   existing JSONPaths file
   */
  final case class Json(info: Info, jsonPaths: BlobStorage.Key) extends ShreddedType {
    def getLoadPath: String =
      s"${info.base}${Common.GoodPrefix}/vendor=${info.vendor}/name=${info.name}/format=json/model=${info.version.model}/revision=${info.version.revision}/addition=${info.version.addition}/"

    def show: String = s"${info.toCriterion.asString} ($jsonPaths)"
  }

  /**
   * Container for S3 folder with shredded TSVs ready to load, without JSONPaths Usually it
   * represents self-describing event or custom/derived context
   *
   * @param info
   *   raw metadata extracted from S3 Key
   */
  final case class Tabular(info: Info) extends ShreddedType {
    def getLoadPath: String =
      s"${info.base}${Common.GoodPrefix}/vendor=${info.vendor}/name=${info.name}/format=tsv/model=${info.version.model}/revision=${info.version.revision}/addition=${info.version.addition}/"

    def show: String = s"${info.toCriterion.asString} TSV"
  }

  final case class Widerow(info: Info) extends ShreddedType {
    def getLoadPath: String = s"${info.base}${Common.GoodPrefix}"

    def show: String = s"${info.toCriterion.asString} WIDEROW"
  }

  /**
   * Raw metadata that can be parsed from S3 Key. It cannot be counted as "final" shredded type, as
   * it's not proven to have JSONPaths file
   *
   * @param base
   *   s3 path run folder
   * @param vendor
   *   self-describing type's vendor
   * @param name
   *   self-describing type's name
   * @param model
   *   self-describing type's SchemaVer model
   * @param entity
   *   what kind of Snowplow entity it is (context or event)
   */
  final case class Info(
    base: BlobStorage.Folder,
    vendor: String,
    name: String,
    version: SchemaVer.Full,
    entity: LoaderMessage.SnowplowEntity
  ) {
    def getSchemaKey: SchemaKey      = SchemaKey(vendor, name, "jsonschema", version)
    def toCriterion: SchemaCriterion = SchemaCriterion(vendor, name, "jsonschema", version.model)

    /** Build valid table name for the shredded type */
    def getName: String =
      s"${toSnakeCase(vendor)}_${toSnakeCase(name)}_${version.model}"

    def getNameFull: String =
      SnowplowEvent.transformSchema(entity.toSdkProperty, vendor, name, version.model)
  }

  /**
   * Transform common shredded type into loader-ready. TSV is isomorphic and cannot fail, but
   * JSONPath-based must have JSONPath file discovered - it's the only possible point of failure
   */
  def fromCommon[F[_]: Monad: Cache: BlobStorage: JsonPathDiscovery](
    base: BlobStorage.Folder,
    jsonpathAssets: Option[BlobStorage.Folder],
    typesInfo: TypesInfo
  ): F[List[DiscoveryStep[ShreddedType]]] =
    typesInfo match {
      case t: TypesInfo.Shredded =>
        t.types.traverse[F, DiscoveryStep[ShreddedType]] {
          case TypesInfo.Shredded.Type(schemaKey, TypesInfo.Shredded.ShreddedFormat.TSV, shredProperty) =>
            val info = Info(base, schemaKey.vendor, schemaKey.name, schemaKey.version, shredProperty)
            (Tabular(info): ShreddedType).asRight[DiscoveryFailure].pure[F]
          case TypesInfo.Shredded.Type(schemaKey, TypesInfo.Shredded.ShreddedFormat.JSON, shredProperty) =>
            val info = Info(base, schemaKey.vendor, schemaKey.name, schemaKey.version, shredProperty)
            Monad[F].map(JsonPathDiscovery[F].discoverJsonPath(jsonpathAssets, info))(_.map(Json(info, _)))
        }
      case t: TypesInfo.WideRow =>
        t.types.traverse[F, DiscoveryStep[ShreddedType]] { case TypesInfo.WideRow.Type(schemaKey, shredProperty) =>
          val info = Info(base, schemaKey.vendor, schemaKey.name, schemaKey.version, shredProperty)
          (Widerow(info): ShreddedType).asRight[DiscoveryFailure].pure[F]
        }
    }
}
