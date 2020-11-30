/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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

import cats.Monad
import cats.data._
import cats.implicits._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaCriterion, SchemaVer}

import com.snowplowanalytics.snowplow.rdbloader.{DiscoveryStep, sequenceInF, LoaderError, LoaderAction, DiscoveryAction}
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, S3, Semver}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{AWS, Cache}
import com.snowplowanalytics.snowplow.rdbloader.common.Common.toSnakeCase

sealed trait ShreddedType {
  /** raw metadata extracted from S3 Key */
  def info: ShreddedType.Info
  /** Get S3 prefix which Redshift should LOAD FROM */
  def getLoadPath: String
  /** Human-readable form */
  def show: String
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
  case class Json(info: Info, jsonPaths: S3.Key) extends ShreddedType {
    def getLoadPath: String =
      s"${info.base}shredded-types/vendor=${info.vendor}/name=${info.name}/format=jsonschema/version=${info.model}-"

    def show: String = s"${info.toCriterion.asString} ($jsonPaths)"
  }

  /**
    * Container for S3 folder with shredded TSVs ready to load, without JSONPaths
    * Usually it represents self-describing event or custom/derived context
    *
    * @param info raw metadata extracted from S3 Key
    */
  case class Tabular(info: Info) extends ShreddedType {
    def getLoadPath: String =
      s"${info.base}shredded-tsv/vendor=${info.vendor}/name=${info.name}/format=jsonschema/version=${info.model}"

    def show: String = s"${info.toCriterion.asString} TSV"
  }

  /**
   * Raw metadata that can be parsed from S3 Key.
   * It cannot be counted as "final" shredded type,
   * as it's not proven to have JSONPaths file
   *
   * @param base s3 path run folder (without `shredded-types` suffix)
   * @param vendor self-describing type's vendor
   * @param name self-describing type's name
   * @param model self-describing type's SchemaVer model
   */
  case class Info(base: S3.Folder, vendor: String, name: String, model: Int, shredJob: Semver) {
    def toCriterion: SchemaCriterion = SchemaCriterion(vendor, name, "jsonschema", model)
  }

  /**
   * Transform common shredded type into loader-ready. TSV is isomorphic and cannot fail,
   * but JSONPath-based must have JSONPath file discovered - it's the only possible point of failure
   */
  def fromCommon[F[_]: Monad: Cache: AWS](base: S3.Folder,
                                          shredJob: Semver,
                                          region: String,
                                          jsonpathAssets: Option[S3.Folder],
                                          commonType: LoaderMessage.ShreddedType): DiscoveryAction[F, ShreddedType] =
    commonType match {
      case LoaderMessage.ShreddedType(schemaKey, LoaderMessage.Format.TSV) =>
        val info = Info(base, schemaKey.vendor, schemaKey.name, schemaKey.version.model, shredJob)
        Monad[F].pure(Tabular(info).asRight[DiscoveryFailure])
      case LoaderMessage.ShreddedType(schemaKey, LoaderMessage.Format.JSON) =>
        val info = Info(base, schemaKey.vendor, schemaKey.name, schemaKey.version.model, shredJob)
        Monad[F].map(discoverJsonPath[F](region, jsonpathAssets, info)) { either =>
          either.map { jsonPath =>
            Json(info, jsonPath)
          }
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
  val ShreddedSubpathPattern =
    ("""shredded\-types""" +
     """/vendor=(?<vendor>[a-zA-Z0-9-_.]+)""" +
     """/name=(?<name>[a-zA-Z0-9-_]+)""" +
     """/format=(?<format>[a-zA-Z0-9-_]+)""" +
     """/version=(?<schemaver>[1-9][0-9]*(?:-(?:0|[1-9][0-9]*)){2})$""").r

  /** Regex to extract `SchemaKey` from `shredded/good` */
  val ShreddedSubpathPatternTabular =
    ("""shredded\-tsv""" +
      """/vendor=(?<vendor>[a-zA-Z0-9-_.]+)""" +
      """/name=(?<name>[a-zA-Z0-9-_]+)""" +
      """/format=(?<format>[a-zA-Z0-9-_]+)""" +
      """/version=(?<model>[1-9][0-9]*)$""").r

  /** Version of legacy Shred job, where TSV output was not possible */
  val ShredJobBeforeTabularVersion = Semver(0,15,0)

  /**
   * "shredded-types" + vendor + name + format + version + filename
   */
  private val MinShreddedPathLengthModern = 6

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
   * Build valid table name for some shredded type
   *
   * @param shreddedType shredded type for self-describing event or context
   * @return valid table name
   */
  def getTableName(shreddedType: ShreddedType): String =
    s"${toSnakeCase(shreddedType.info.vendor)}_${toSnakeCase(shreddedType.info.name)}_${shreddedType.info.model}"

  /**
   * Check that JSONPaths file exists in Snowplow hosted assets bucket
   *
   * @param s3Region hosted assets region
   * @param key vendor dir and filename, e.g. `com.acme/event_1`
   * @return full S3 key if file exists, discovery error otherwise
   */
  def getSnowplowJsonPath[F[_]: Monad: AWS: Cache](s3Region: String,
                                                   key: String): DiscoveryAction[F, S3.Key] = {
    val hostedAssetsBucket = getHostedAssetsBucket(s3Region)
    val fullDir = S3.Folder.append(hostedAssetsBucket, JsonpathsPath)
    val s3Key = S3.Key.coerce(fullDir + key)
    AWS[F].keyExists(s3Key).flatMap {
      case true =>
        Cache[F].putCache(key, Some(s3Key)).as(s3Key.asRight)
      case false =>
        Cache[F].putCache(key, None).as(DiscoveryFailure.JsonpathDiscoveryFailure(key).asLeft)
    }
  }

  /** Discover multiple JSONPaths for shredded types at once and turn into `LoaderAction` */
  def discoverBatch[F[_]: Monad: Cache: AWS](region: String,
                                             jsonpathAssets: Option[S3.Folder],
                                             raw: List[ShreddedType.Info]): LoaderAction[F, List[ShreddedType]] = {
    // Discover data for single item
    def discover(info: ShreddedType.Info): F[ValidatedNel[DiscoveryFailure, ShreddedType]] = {
      val jsonpaths: F[DiscoveryStep[S3.Key]] = ShreddedType.discoverJsonPath[F](region, jsonpathAssets, info)
      val shreddedType = jsonpaths.map(_.map(s3key => ShreddedType.Json(info, s3key)))
      shreddedType.map(_.toValidatedNel)
    }

    val action: F[Either[LoaderError, List[ShreddedType]]] =
      sequenceInF(raw.traverse(discover), LoaderError.flattenValidated[List[ShreddedType]])

    LoaderAction(action)
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
   * Parse S3 key path into shredded type
   *
   * @param key valid S3 key
   * @param shredJob version of shred job to decide what path format should be present
   * @return either discovery failure or info (which in turn can be tabular (true) or JSON (false))
   */
  def transformPath(key: S3.Key, shredJob: Semver): Either[DiscoveryFailure, (Boolean, Info)] = {
    val (bucket, path) = S3.splitS3Key(key)
    val (subpath, shredpath) = splitFilpath(path)
    extractSchemaKey(shredpath, shredJob) match {
      case Some(Extracted.Legacy(SchemaKey(vendor, name, _, SchemaVer.Full(model, _, _)))) =>
        val prefix = S3.Folder.coerce("s3://" + bucket + "/" + subpath)
        val result = Info(prefix, vendor, name, model, shredJob)
        (false, result).asRight
      case Some(Extracted.Tabular(vendor, name, _, model)) =>
        val prefix = S3.Folder.coerce("s3://" + bucket + "/" + subpath)
        val result = Info(prefix, vendor, name, model, shredJob)
        (true, result).asRight
      case None =>
        DiscoveryFailure.ShreddedTypeKeyFailure(key).asLeft
    }
  }

  sealed trait Extracted
  object Extracted {
    case class Legacy(key: SchemaKey) extends Extracted
    case class Tabular(vendor: String, name: String, format: String, model: Int) extends Extracted
  }

  /**
   * Extract `SchemaKey` from subpath, which can be
   * json-style (post-0.12.0) vendor=com.acme/name=schema-name/format=jsonschema/version=1-0-0
   * tsv-style (post-0.16.0) vendor=com.acme/name=schema-name/format=jsonschema/version=1
   * This function transforms any of above valid paths to `SchemaKey`
   *
   * @param subpath S3 subpath of four `SchemaKey` elements
   * @param shredJob shred job version to decide what format should be present
   * @return valid schema key if found
   */
  def extractSchemaKey(subpath: String, shredJob: Semver): Option[Extracted] =
    subpath match {
      case ShreddedSubpathPattern(vendor, name, format, version) =>
        val uri = s"iglu:$vendor/$name/$format/$version"
        SchemaKey.fromUri(uri).toOption.map(Extracted.Legacy)
      case ShreddedSubpathPatternTabular(vendor, name, format, model) if shredJob >= ShredJobBeforeTabularVersion =>
        scala.util.Try(model.toInt).toOption match {
          case Some(m) => Extracted.Tabular(vendor, name, format, m).some
          case None => None
        }
      case _ => None
    }

  /**
   * Split S3 filepath (without bucket name) into subpath and shreddedpath
   * Works both for legacy and modern format. Omits file
   *
   * `path/to/shredded/good/run=2017-05-02-12-30-00/vendor=com.acme/name=event/format=jsonschema/version=1-0-0/part-0001`
   * ->
   * `(path/to/shredded/good/run=2017-05-02-12-30-00/, vendor=com.acme/name=event/format=jsonschema/version=1-0-0)`
   *
   * @param path S3 key without bucket name
   * @return pair of subpath and shredpath
   */
  private def splitFilpath(path: String): (String, String) =
    path.split("/").reverse.splitAt(MinShreddedPathLengthModern) match {
      case (reverseSchema, reversePath) =>
        (reversePath.reverse.mkString("/"), reverseSchema.tail.reverse.mkString("/"))
    }
}
