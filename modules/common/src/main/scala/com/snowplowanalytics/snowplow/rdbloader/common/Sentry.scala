/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.common

import java.net.URI

import cats.implicits._
import cats.effect._

import io.sentry.{Sentry => SentryOrg, SentryClient, SentryOptions}

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object Sentry {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def init[F[_]: Sync](dsn: Option[URI]): Resource[F, Option[SentryClient]] =
    dsn match {
      case Some(uri) =>
        val acquire = Sync[F].delay(SentryOrg.init(SentryOptions.defaults(uri.toString)))
        Resource
          .make(acquire)(client => Sync[F].delay(client.closeConnection()))
          .map(_.some)
          .evalTap { _ =>
            Logger[F].info(s"Sentry has been initialised at $uri")
          }

      case None =>
        Resource.pure[F, Option[SentryClient]](none[SentryClient])
    }
}
