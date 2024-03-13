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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import cats.implicits._
import cats.effect.Sync
import cats.effect.kernel.Ref
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage

trait Cache[F[_]] {

  /** Put value into cache (stored in interpreter) */
  def putCache(key: String, value: Option[BlobStorage.Key]): F[Unit]

  /** Get value from cache (stored in interpreter) */
  def getCache(key: String): F[Option[Option[BlobStorage.Key]]]
}

object Cache {
  def apply[F[_]](implicit ev: Cache[F]): Cache[F] = ev

  def cacheInterpreter[F[_]: Sync](cache: Ref[F, Map[String, Option[BlobStorage.Key]]]): Cache[F] =
    new Cache[F] {
      def getCache(key: String): F[Option[Option[BlobStorage.Key]]] =
        cache.get.map(_.get(key))

      def putCache(key: String, value: Option[BlobStorage.Key]): F[Unit] =
        cache.update(c => c ++ Map(key -> value))
    }
}
