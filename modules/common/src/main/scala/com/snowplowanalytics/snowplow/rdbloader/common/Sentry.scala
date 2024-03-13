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
