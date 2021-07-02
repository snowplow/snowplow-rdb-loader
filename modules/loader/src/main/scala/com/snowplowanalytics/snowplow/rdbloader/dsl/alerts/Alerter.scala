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

package com.snowplowanalytics.snowplow.rdbloader.dsl.alerts

import cats.Applicative
import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits.igluNormalizeDataJson
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring.{AlertPayload, AlertSchemaKey}
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import io.circe.Json
import io.circe.syntax.EncoderOps

trait Alerter[F[_]] {
  def alert(payload: AlertPayload): F[Unit]
}

object Alerter {
  def noop[F[_]: Applicative]: Alerter[F] = new Alerter[F] {
    def alert(payload: AlertPayload): F[Unit] = Applicative[F].unit
  }

  def createPostPayload(payload: AlertPayload): String =
    SelfDescribingData[Json](AlertSchemaKey, payload.asJson).normalize.noSpaces

  def createAlertPayload(folder: Folder, message: String, tags: Map[String, String]): AlertPayload =
    AlertPayload(BuildInfo.version, folder , message, tags)
}
