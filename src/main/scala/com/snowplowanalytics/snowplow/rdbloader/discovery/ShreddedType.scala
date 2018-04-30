/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader
package discovery

import cats.data._
import cats.free.Free
import cats.implicits._

import com.snowplowanalytics.iglu.client.SchemaCriterion
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.snowplow.rdbloader.LoaderError._
import com.snowplowanalytics.snowplow.rdbloader.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.utils.Common.toSnakeCase

/**
 * Container for S3 folder with shredded JSONs ready to load
 * Usually it represents self-describing event or custom/derived context
 *
 * @param info raw metadata extracted from S3 Key
 * @param jsonPaths existing JSONPaths file
 */
case class ShreddedType(info: ShreddedType.Info, jsonPaths: S3.Key) {
  /** Get S3 prefix which Redshift should LOAD FROM */
  def getLoadPath: String = {
   if (info.shredJob <= ShreddedType.ShredJobBeforeSparkVersion) {
     s"${info.base}${info.vendor}/${info.name}/jsonschema/${info.model}-"
   } else {
     s"${info.base}shredded-types/vendor=${info.vendor}/name=${info.name}/format=jsonschema/version=${info.model}-"
   }
  }

  /** Human-readable form */
  def show: String = s"${info.toCriterion.toString} ($jsonPaths)"
}

/**
 * Companion object for `ShreddedType` containing discovering functions
 */
object ShreddedType {

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
   * Basis for Snowplow hosted assets bucket.
   * Can be modified to match specific region
   */
  val SnowplowHostedAssetsRoot = "s3://snowplow-hosted-assets"

  /**
   * Default JSONPaths path
   */
  val JsonpathsPath = "4-storage/redshift-storage/jsonpaths/"

  /**
   * Regex to extract `SchemaKey` from `shredded/good`
   */
  val ShreddedSubpathPattern =
    ("""shredded\-types""" +
     """/vendor=(?<vendor>[a-zA-Z0-9-_.]+)""" +
     """/name=(?<name>[a-zA-Z0-9-_]+)""" +
     """/format=(?<format>[a-zA-Z0-9-_]+)""" +
     """/version=(?<schemaver>[1-9][0-9]*(?:-(?:0|[1-9][0-9]*)){2})$""").r

  /**
   * Version of legacy Shred job, where old path pattern was used
   * `com.acme/event/jsonschema/1-0-0`
   */
  val ShredJobBeforeSparkVersion = Semver(0,11,0)

  /**
   * vendor + name + format + version + filename
   */
  private val MinShreddedPathLengthLegacy = 5

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
  def discoverJsonPath(region: String, jsonpathAssets: Option[S3.Folder], shreddedType: Info): DiscoveryAction[S3.Key] = {
    val filename = s"""${toSnakeCase(shreddedType.name)}_${shreddedType.model}.json"""
    val key = s"${shreddedType.vendor}/$filename"

    LoaderA.getCache(key).flatMap { (value: Option[Option[S3.Key]]) =>
      value match {
        case Some(Some(jsonPath)) =>
          Free.pure(jsonPath.asRight)
        case Some(None) =>
          Free.pure(JsonpathDiscoveryFailure(key).asLeft)
        case None =>
          jsonpathAssets match {
            case Some(assets) =>
              val path = S3.Folder.append(assets, shreddedType.vendor)
              val s3Key = S3.Key.coerce(path + filename)
              LoaderA.keyExists(s3Key).flatMap {
                case true =>
                  for {
                    _ <- LoaderA.putCache(key, Some(s3Key))
                  } yield s3Key.asRight
                case false =>
                  getSnowplowJsonPath(region, key)
              }
            case None =>
              getSnowplowJsonPath(region, key)
          }
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
  def getSnowplowJsonPath(s3Region: String, key: String): DiscoveryAction[S3.Key] = {
    val hostedAssetsBucket = getHostedAssetsBucket(s3Region)
    val fullDir = S3.Folder.append(hostedAssetsBucket, JsonpathsPath)
    val s3Key = S3.Key.coerce(fullDir + key)
    LoaderA.keyExists(s3Key).flatMap {
      case true =>
        for {
          _ <- LoaderA.putCache(key, Some(s3Key))
        } yield s3Key.asRight
      case false =>
        for {
          _ <- LoaderA.putCache(key, None)
        } yield JsonpathDiscoveryFailure(key).asLeft
    }
  }

  /** Discover multiple JSONPaths for shredded types at once and turn into `LoaderAction` */
  def discoverBatch(region: String,
                    jsonpathAssets: Option[S3.Folder],
                    raw: List[ShreddedType.Info]): LoaderAction[List[ShreddedType]] = {
    // Discover data for single item
    def discover(info: ShreddedType.Info): Action[ValidatedNel[DiscoveryFailure, ShreddedType]] = {
      val jsonpaths = ShreddedType.discoverJsonPath(region, jsonpathAssets, info)
      val shreddedType = jsonpaths.map(_.map(s3key => ShreddedType(info, s3key)))
      shreddedType.map(_.toValidatedNel)
    }

    val action: Action[Either[LoaderError, List[ShreddedType]]] =
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
   * @return either discovery failure
   */
  def transformPath(key: S3.Key, shredJob: Semver): Either[DiscoveryFailure, Info] = {
    val (bucket, path) = S3.splitS3Key(key)
    val (subpath, shredpath) = splitFilpath(path, shredJob)
    extractSchemaKey(shredpath, shredJob) match {
      case Some(SchemaKey(vendor, name, _, SchemaVer.Full(model, _, _))) =>
        val prefix = S3.Folder.coerce("s3://" + bucket + "/" + subpath)
        val result = Info(prefix, vendor, name, model, shredJob)
        result.asRight
      case _ =>
        ShreddedTypeKeyFailure(key).asLeft
    }
  }

  /**
   * Extract `SchemaKey` from subpath, which can be
   * legacy-style (pre-0.12.0) com.acme/schema-name/jsonschema/1-0-0 or
   * modern-style (post-0.12.0) vendor=com.acme/name=schema-name/format=jsonschema/version=1-0-0
   * This function transforms any of above valid paths to `SchemaKey`
   *
   * @param subpath S3 subpath of four `SchemaKey` elements
   * @param shredJob shred job version to decide what format should be present
   * @return valid schema key if found
   */
  def extractSchemaKey(subpath: String, shredJob: Semver): Option[SchemaKey] = {
    if (shredJob <= ShredJobBeforeSparkVersion) {
      val uri = "iglu:" + subpath
      SchemaKey.fromUri(uri)
    } else subpath match {
      case ShreddedSubpathPattern(vendor, name, format, version) =>
        val uri = s"iglu:$vendor/$name/$format/$version"
        SchemaKey.fromUri(uri)
      case _ => None
    }
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
  private def splitFilpath(path: String, shredJob: Semver): (String, String) = {
    if (shredJob <= ShredJobBeforeSparkVersion) {
      path.split("/").reverse.splitAt(MinShreddedPathLengthLegacy) match {
        case (reverseSchema, reversePath) =>
          (reversePath.reverse.mkString("/"), reverseSchema.tail.reverse.mkString("/"))
      }
    } else {
      path.split("/").reverse.splitAt(MinShreddedPathLengthModern) match {
        case (reverseSchema, reversePath) =>
          (reversePath.reverse.mkString("/"), reverseSchema.tail.reverse.mkString("/"))
      }
    }
  }
}
