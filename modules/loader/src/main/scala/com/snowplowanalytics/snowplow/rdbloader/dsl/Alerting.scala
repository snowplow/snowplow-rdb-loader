/*
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd. All rights reserved.
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

package com.snowplowanalytics.snowplow.rdbloader.dsl

import cats.Applicative
import cats.implicits._

import cats.effect.{Sync, ContextShift}

import org.http4s.{Request, MediaType, Method}
import org.http4s.client.Client
import org.http4s.headers.`Content-Type`

import com.snowplowanalytics.snowplow.rdbloader.common.config.Config
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring.AlertPayload


trait Alerting[F[_]] {
  def alert(payload: AlertPayload): F[Unit]
}

object Alerting {

  def apply[F[_]](implicit ev: Alerting[F]): Alerting[F] = ev

  def noop[F[_]: Applicative]: Alerting[F] =
    (_: AlertPayload) => Applicative[F].unit

  def webhook[F[_]: ContextShift: Sync: Logging](webhookConfig: Option[Config.Webhook], httpClient: Client[F]): Alerting[F] =
    webhookConfig match {
      case Some(webhook) =>
        (payload: Monitoring.AlertPayload) => {
          val body = payload.toByteStream.covary[F]
          val request: Request[F] =
            Request[F](Method.POST, webhook.endpoint)
              .withEntity(body)
              .withContentType(`Content-Type`(MediaType.application.json))

          httpClient
            .status(request)
            .map(_.isSuccess)
            .ifM(Sync[F].unit, Logging[F].error(s"Webhook ${webhook.endpoint} returned non-2xx response"))
        }
        case None => Alerting.noop[F]
    }
}
