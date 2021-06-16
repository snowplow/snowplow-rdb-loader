/*
 * Copyright (c) 2021-2021 Snowplow Analytics Ltd. All rights reserved.
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

package com.snowplowanalytics.snowplow.rdbloader.dsl.alerts

import cats.implicits._
import cats.effect.{Blocker, ContextShift, Sync}
import com.snowplowanalytics.snowplow.rdbloader.common.config.Config.Webhook
import com.snowplowanalytics.snowplow.rdbloader.dsl.alerts.Alerter.createPostPayload
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, Monitoring}
import fs2.Stream
import org.http4s.client.Client
import org.http4s.headers.`Content-Type`
import org.http4s.{MediaType, Method, Request}

object WebhookAlerter {
  def build[F[_]: ContextShift: Sync: Logging](webhookConfig: Option[Webhook], blocker: Blocker, httpClient: Client[F]): Alerter[F] =
    webhookConfig match {
      case Some(webhook) =>
        new Alerter[F] {
          def alert(payload: Monitoring.AlertPayload): F[Unit] =
              blocker.blockOn{
                val body = Stream.emit(createPostPayload(payload).getBytes).covary[F]
                val request: Request[F] =
                  Request[F](Method.POST, webhook.endpoint)
                    .withEntity(body)
                    .withContentType(`Content-Type`(MediaType.application.json))

                httpClient
                  .status(request)
                  .flatMap{ status =>
                    if ((status.code / 100) != 2) Logging[F].info(s"Webhook [${webhook.endpoint} returned non-2xx response [${status.code}]]")
                    else Sync[F].unit
                  }
                }
              }
      case None => Alerter.noop[F]
    }
}
