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

import scala.util.matching.Regex

import cats.Monad
import cats.implicits._

import com.snowplowanalytics.iglu.core.SchemaCriterion

import com.snowplowanalytics.snowplow.rdbloader.{DiscoveryAction, DiscoveryStep}
import com.snowplowanalytics.snowplow.rdbloader.common.{S3, LoaderMessage, Common}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{TypesInfo, SnowplowEntity}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{AWS, Cache}
import com.snowplowanalytics.snowplow.rdbloader.common.Common.toSnakeCase

/**
 * Generally same as `LoaderMessage.ShreddedType`, but for JSON types
 * holds an information about discovered JSONPath file and does NOT
 * contain full SchemaVer
 *
 * Can be converted from `LoaderMessage.ShreddedType`
 * using `DataDiscover.fromLoaderMessage`
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
    case ShreddedType.Tabular(ShreddedType.Info(_, vendor, name, model, _)) =>
      vendor == Common.AtomicSchema.vendor && name == Common.AtomicSchema.name && model == Common.AtomicSchema.version.model
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
    * Container for S3 folder with shredded JSONs ready to load with JSONPaths
    * Usually it represents self-describing event or custom/derived context
    *
    * @param jsonPaths existing JSONPaths file
    */
  final case class Json(info: Info, jsonPaths: S3.Key) extends ShreddedType {
    def getLoadPath: String =
      s"${info.base}${Common.GoodPrefix}/vendor=${info.vendor}/name=${info.name}/format=json/model=${info.model}"

    def show: String = s"${info.toCriterion.asString} ($jsonPaths)"
  }

  /**
    * Container for S3 folder with shredded TSVs ready to load, without JSONPaths
    * Usually it represents self-describing event or custom/derived context
    *
    * @param info raw metadata extracted from S3 Key
    */
  final case class Tabular(info: Info) extends ShreddedType {
    def getLoadPath: String =
      s"${info.base}${Common.GoodPrefix}/vendor=${info.vendor}/name=${info.name}/format=tsv/model=${info.model}"

    def show: String = s"${info.toCriterion.asString} TSV"
  }

  final case class Widerow(info: Info) extends ShreddedType {
    def getLoadPath: String = s"${info.base}${Common.GoodPrefix}"

    def show: String = s"${info.toCriterion.asString} WIDEROW"
  }

  /**
   * Raw metadata that can be parsed from S3 Key.
   * It cannot be counted as "final" shredded type,
   * as it's not proven to have JSONPaths file
   *
   * @param base s3 path run folder
   * @param vendor self-describing type's vendor
   * @param name self-describing type's name
   * @param model self-describing type's SchemaVer model
   * @param entity what kind of Snowplow entity it is (context or event)
   */
  final case class Info(base: S3.Folder, vendor: String, name: String, model: Int, entity: LoaderMessage.SnowplowEntity) {
    def toCriterion: SchemaCriterion = SchemaCriterion(vendor, name, "jsonschema", model)

    /** Build valid table name for the shredded type */
    def getName: String =
      s"${toSnakeCase(vendor)}_${toSnakeCase(name)}_$model"

    def getNameFull: String = {
      val isContext = entity == SnowplowEntity.Context
      (if (isContext) "contexts_" else "unstruct_event_") ++ getName
    }
  }

  /**
   * Transform common shredded type into loader-ready. TSV is isomorphic and cannot fail,
   * but JSONPath-based must have JSONPath file discovered - it's the only possible point of failure
   */
  def fromCommon[F[_]: Monad: Cache: AWS](base: S3.Folder,
                                          region: String,
                                          jsonpathAssets: Option[S3.Folder],
                                          typesInfo: TypesInfo): F[List[DiscoveryStep[ShreddedType]]] =
    typesInfo match {
      case t: TypesInfo.Shredded =>
        t.types.traverse[F, DiscoveryStep[ShreddedType]] {
          case TypesInfo.Shredded.Type(schemaKey, TypesInfo.Shredded.ShreddedFormat.TSV, shredProperty) =>
            val info = Info(base, schemaKey.vendor, schemaKey.name, schemaKey.version.model, shredProperty)
            (Tabular(info): ShreddedType).asRight[DiscoveryFailure].pure[F]
          case TypesInfo.Shredded.Type(schemaKey, TypesInfo.Shredded.ShreddedFormat.JSON, shredProperty) =>
            val info = Info(base, schemaKey.vendor, schemaKey.name, schemaKey.version.model, shredProperty)
            Monad[F].map(discoverJsonPath[F](region, jsonpathAssets, info))(_.map(Json(info, _)))
        }
      case t: TypesInfo.WideRow =>
        t.types.traverse[F, DiscoveryStep[ShreddedType]] {
          case TypesInfo.WideRow.Type(schemaKey, shredProperty) =>
            val info = Info(base, schemaKey.vendor, schemaKey.name, schemaKey.version.model, shredProperty)
            (Widerow(info): ShreddedType).asRight[DiscoveryFailure].pure[F]
        }
    }

  /**
   * Basis for Snowplow hosted assets bucket.
   * Can be modified to match specific region
   */
  val SnowplowHostedAssetsRoot = "s3://snowplow-hosted-assets"

  /**
   * Default JSONPaths path
   */
  val JsonpathsPath = "4-storage/redshift-storage/jsonpaths/"

  /** Regex to extract `SchemaKey` from `shredded/good` */
  val ShreddedSubpathPattern: Regex =
    ("""vendor=(?<vendor>[a-zA-Z0-9-_.]+)""" +
     """/name=(?<name>[a-zA-Z0-9-_]+)""" +
     """/format=json""" +
     """/model=(?<model>[1-9][0-9]*)$""").r

  /** Regex to extract `SchemaKey` from `shredded/good` */
  val ShreddedSubpathPatternTabular: Regex =
    ("""vendor=(?<vendor>[a-zA-Z0-9-_.]+)""" +
     """/name=(?<name>[a-zA-Z0-9-_]+)""" +
     """/format=tsv""" +
     """/model=(?<model>[1-9][0-9]*)$""").r

  /**
   * Check where JSONPaths file for particular shredded type exists:
   * in cache, in custom `s3.buckets.jsonpath_assets` S3 path or in Snowplow hosted assets bucket
   * and return full JSONPaths S3 path
   *
   * @param shreddedType some shredded type (self-describing event or context)
   * @return full valid s3 path (with `s3://` prefix)
   */
  def discoverJsonPath[F[_]: Monad: Cache: AWS](region: String, jsonpathAssets: Option[S3.Folder], shreddedType: Info): DiscoveryAction[F, S3.Key] = {
    val filename = s"""${toSnakeCase(shreddedType.name)}_${shreddedType.model}.json"""
    val key = s"${shreddedType.vendor}/$filename"

    Cache[F].getCache(key).flatMap {
      case Some(Some(jsonPath)) =>
        Monad[F].pure(jsonPath.asRight)
      case Some(None) =>
        Monad[F].pure(DiscoveryFailure.JsonpathDiscoveryFailure(key).asLeft)
      case None =>
        jsonpathAssets match {
          case Some(assets) =>
            val path = S3.Folder.append(assets, shreddedType.vendor)
            val s3Key = S3.Key.coerce(path + filename)
            AWS[F].keyExists(s3Key).flatMap {
              case true =>
                Cache[F].putCache(key, Some(s3Key)).as(s3Key.asRight)
              case false =>
                getSnowplowJsonPath[F](region, key)
            }
          case None =>
            getSnowplowJsonPath[F](region, key)
        }
    }
  }

  /**
   * Check that JSONPaths file exists in Snowplow hosted assets bucket
   *
   * @param s3Region hosted assets region
   * @param key vendor dir and filename, e.g. `com.acme/event_1`
   * @return full S3 key if file exists, discovery error otherwise
   */
  def getSnowplowJsonPath[F[_]: Monad: AWS: Cache](s3Region: String,
                                                   key: String): DiscoveryAction[F, S3.Key] = {
    val fullDir = S3.Folder.append(getHostedAssetsBucket(s3Region), JsonpathsPath)
    val s3Key = S3.Key.coerce(fullDir + key)
    AWS[F].keyExists(s3Key).ifM(
      Cache[F].putCache(key, Some(s3Key)).as(s3Key.asRight[DiscoveryFailure]),
      Cache[F].putCache(key, None).as(DiscoveryFailure.JsonpathDiscoveryFailure(key).asLeft[S3.Key])
    )
  }

  /**
   * Get Snowplow hosted assets S3 bucket for specific region
   *
   * @param region valid AWS region
   * @return AWS S3 path such as `s3://snowplow-hosted-assets-us-west-2/`
   */
  def getHostedAssetsBucket(region: String): S3.Folder = {
    val suffix = if (region == "eu-west-1") "" else s"-$region"
    S3.Folder.coerce(s"$SnowplowHostedAssetsRoot$suffix")
  }

  /**
   * Extract `SchemaKey` from subpath, which can be
   * json-style (post-0.12.0) vendor=com.acme/name=schema-name/format=jsonschema/version=1-0-0
   * tsv-style (post-0.16.0) vendor=com.acme/name=schema-name/format=jsonschema/version=1
   * This function transforms any of above valid paths to `SchemaKey`
   *
   * @param subpath S3 subpath of four `SchemaKey` elements
   * @return valid schema key if found
   */
  def extractSchemaKey(subpath: String): Option[(String, String, Int, TypesInfo.Shredded.ShreddedFormat)] =
    subpath match {
      case ShreddedSubpathPattern(vendor, name, model) =>
        scala.util.Try(model.toInt).toOption match {
          case Some(m) => Some((vendor, name, m, TypesInfo.Shredded.ShreddedFormat.JSON))
          case None => None
        }
      case ShreddedSubpathPatternTabular(vendor, name, model) =>
        scala.util.Try(model.toInt).toOption match {
          case Some(m) => Some((vendor, name, m, TypesInfo.Shredded.ShreddedFormat.TSV))
          case None => None
        }
      case _ =>
        None
    }
}
