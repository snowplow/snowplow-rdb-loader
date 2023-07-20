/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.cloud

import cats.Monad
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.DiscoveryAction
import com.snowplowanalytics.snowplow.rdbloader.common.Common.toSnakeCase
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.discovery.DiscoveryFailure
import com.snowplowanalytics.snowplow.rdbloader.discovery.DiscoveryFailure.JsonpathDiscoveryFailure
import com.snowplowanalytics.snowplow.rdbloader.discovery.ShreddedType.Info
import com.snowplowanalytics.snowplow.rdbloader.dsl.Cache

trait JsonPathDiscovery[F[_]] {
  def discoverJsonPath(jsonpathAssets: Option[BlobStorage.Folder], shreddedType: Info): DiscoveryAction[F, BlobStorage.Key]
}

object JsonPathDiscovery {
  def apply[F[_]](implicit ev: JsonPathDiscovery[F]): JsonPathDiscovery[F] = ev

  def aws[F[_]: Monad: Cache: BlobStorage](region: String): JsonPathDiscovery[F] = new JsonPathDiscovery[F] {

    /**
     * Basis for Snowplow hosted assets bucket. Can be modified to match specific region
     */
    val SnowplowHostedAssetsRoot = "s3://snowplow-hosted-assets"

    /**
     * Default JSONPaths path
     */
    val JsonpathsPath = "4-storage/redshift-storage/jsonpaths/"

    /**
     * Check where JSONPaths file for particular shredded type exists: in cache, in custom
     * `s3.buckets.jsonpath_assets` S3 path or in Snowplow hosted assets bucket and return full
     * JSONPaths S3 path
     *
     * @param shreddedType
     *   some shredded type (self-describing event or context)
     * @return
     *   full valid s3 path (with `s3://` prefix)
     */
    override def discoverJsonPath(jsonpathAssets: Option[BlobStorage.Folder], shreddedType: Info): DiscoveryAction[F, BlobStorage.Key] = {
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
              val path = BlobStorage.Folder.append(assets, shreddedType.vendor)
              val s3Key = BlobStorage.Key.coerce(path + filename)
              BlobStorage[F].keyExists(s3Key).flatMap {
                case true =>
                  Cache[F].putCache(key, Some(s3Key)).as(s3Key.asRight)
                case false =>
                  getSnowplowJsonPath(key)
              }
            case None =>
              getSnowplowJsonPath(key)
          }
      }
    }

    /**
     * Check that JSONPaths file exists in Snowplow hosted assets bucket
     *
     * @param s3Region
     *   hosted assets region
     * @param key
     *   vendor dir and filename, e.g. `com.acme/event_1`
     * @return
     *   full S3 key if file exists, discovery error otherwise
     */
    private def getSnowplowJsonPath(key: String): DiscoveryAction[F, BlobStorage.Key] = {
      val fullDir = BlobStorage.Folder.append(getHostedAssetsBucket(region), JsonpathsPath)
      val s3Key = BlobStorage.Key.coerce(fullDir + key)
      BlobStorage[F]
        .keyExists(s3Key)
        .ifM(
          Cache[F].putCache(key, Some(s3Key)).as(s3Key.asRight[DiscoveryFailure]),
          Cache[F].putCache(key, None).as(DiscoveryFailure.JsonpathDiscoveryFailure(key).asLeft[BlobStorage.Key])
        )
    }

    /**
     * Get Snowplow hosted assets S3 bucket for specific region
     *
     * @param region
     *   valid AWS region
     * @return
     *   AWS S3 path such as `s3://snowplow-hosted-assets-us-west-2/`
     */
    private def getHostedAssetsBucket(region: String): BlobStorage.Folder = {
      val suffix = if (region == "eu-west-1") "" else s"-$region"
      BlobStorage.Folder.coerce(s"$SnowplowHostedAssetsRoot$suffix")
    }
  }

  def noop[F[_]: Monad]: JsonPathDiscovery[F] = new JsonPathDiscovery[F] {
    override def discoverJsonPath(jsonpathAssets: Option[BlobStorage.Folder], shreddedType: Info): DiscoveryAction[F, BlobStorage.Key] =
      Monad[F].pure(JsonpathDiscoveryFailure("").asLeft[BlobStorage.Key]).asInstanceOf[DiscoveryAction[F, BlobStorage.Key]]
  }
}
