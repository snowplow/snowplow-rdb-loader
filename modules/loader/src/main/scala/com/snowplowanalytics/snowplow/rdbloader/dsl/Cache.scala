/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
