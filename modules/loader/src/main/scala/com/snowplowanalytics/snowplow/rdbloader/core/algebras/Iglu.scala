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
package com.snowplowanalytics.snowplow.rdbloader.core.algebras

import cats.effect.{Async, Clock, Resource}
import cats.implicits._
import io.circe.Json
import io.circe.syntax._
import org.http4s.client.Client

import com.snowplowanalytics.iglu.client.{Client => IgluClient}
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, RegistryLookup}
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList
import com.snowplowanalytics.snowplow.badrows.FailureDetails.LoaderIgluError
import com.snowplowanalytics.snowplow.rdbloader.common._
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Flattening
import com.snowplowanalytics.snowplow.rdbloader.core.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.core.discovery.DiscoveryFailure

trait Iglu[F[_]] {

  /** Retrieve list of schemas from Iglu Server */
  def getSchemas(vendor: String, name: String, model: Int): F[Either[LoaderError, SchemaList]]
}

object Iglu {
  def apply[F[_]](implicit ev: Iglu[F]): Iglu[F] = ev

  def igluInterpreter[F[_]: Async: Clock](httpClient: Client[F], igluConfig: Json): Resource[F, Iglu[F]] = {
    implicit val registryLookup: RegistryLookup[F] =
      Http4sRegistryLookup[F](httpClient)

    val buildResolver: F[Resolver[F]] =
      IgluClient
        .parseDefault[F](igluConfig)
        .map(_.resolver)
        .leftMap { decodingFailure =>
          new IllegalArgumentException(s"Cannot initialize Iglu Resolver: ${decodingFailure.show}")
        }
        .rethrowT

    Resource.eval[F, Resolver[F]](buildResolver).map[F, Iglu[F]] {
      resolver => (vendor: String, name: String, model: Int) =>
        val attempt = Flattening.getOrdered[F](resolver, vendor, name, model)
        attempt
          .recoverWith {
            case LoaderIgluError.SchemaListNotFound(_, error) if isInputError(error) =>
              attempt
          }
          .leftMap { resolutionError =>
            val message =
              s"Cannot get schemas for iglu:$vendor/$name/jsonschema/$model-*-*  ${resolutionError.asJson.noSpaces}"
            LoaderError.DiscoveryError(DiscoveryFailure.IgluError(message))
          }
          .value
    }
  }
}
