/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.storage

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

import cats.Id

import io.circe.Json

import com.snowplowanalytics.iglu.client.{Client, Resolver}

import com.snowplowanalytics.snowplow.storage.spark.DynamodbManifest.ManifestFailure

package object spark {

  private val Formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  implicit class ResolverOps(resolver: Resolver[Id]) {
    def cacheless: Resolver[ManifestFailure] =
      Resolver(resolver.repos, None)
  }

  implicit class ClientOps(client: Client[Id, Json]) {
    def cacheless: Client[ManifestFailure, Json] =
      Client(client.resolver.cacheless, client.validator)
  }

  implicit class InstantOps(time: Instant) {
    def formatted: String = {
      time.atOffset(ZoneOffset.UTC).format(Formatter)
    }
  }
}
