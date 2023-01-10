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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import cats.data.EitherT
import cats.~>
import cats.implicits._
import cats.effect.{Async, Resource}
import io.circe.Json
import io.circe.syntax._
import org.http4s.client.Client
import com.snowplowanalytics.iglu.client.resolver.{Resolver, ResolverCache}
import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, RegistryLookup}
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList
import com.snowplowanalytics.snowplow.badrows.FailureDetails.LoaderIgluError
import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow
import com.snowplowanalytics.snowplow.rdbloader.common._
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Flattening
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.NonAtomicFieldsProvider
import com.snowplowanalytics.snowplow.rdbloader.discovery.DiscoveryFailure

trait Iglu[F[_]] { self =>

  /** Retrieve list of schemas from Iglu Server */
  def getSchemas(
    vendor: String,
    name: String,
    model: Int
  ): F[Either[LoaderError, SchemaList]]

  def fieldNamesFromTypes(types: List[WideRow.Type]): EitherT[F, LoaderIgluError, List[String]]

  def mapK[G[_]](arrow: F ~> G): Iglu[G] = new Iglu[G] {

    /** Retrieve list of schemas from Iglu Server */
    override def getSchemas(
      vendor: String,
      name: String,
      model: Int
    ): G[Either[LoaderError, SchemaList]] = arrow(self.getSchemas(vendor, name, model))

    override def fieldNamesFromTypes(types: List[WideRow.Type]): EitherT[G, LoaderIgluError, List[String]] =
      self.fieldNamesFromTypes(types).mapK(arrow)
  }
}

object Iglu {
  def apply[F[_]](implicit ev: Iglu[F]): Iglu[F] = ev

  def igluInterpreter[F[_]: Async](httpClient: Client[F], igluConfig: Json): Resource[F, Iglu[F]] = {
    implicit val registryLookup: RegistryLookup[F] =
      Http4sRegistryLookup[F](httpClient)

    val buildResolver: F[Resolver[F]] =
      Resolver
        .parse[F](igluConfig)
        .map {
          _.map(_.copy(cache = none[ResolverCache[F]])) // Disable cache to not re-fetch the stale state
            .leftMap(decodingFailure => new IllegalArgumentException(s"Cannot initialize Iglu Resolver: ${decodingFailure.show}"))
        }
        .rethrow

    Resource.eval(buildResolver).map { resolver =>
      new Iglu[F] {
        def getSchemas(
          vendor: String,
          name: String,
          model: Int
        ): F[Either[LoaderError, SchemaList]] = {
          val attempt = Flattening.getOrdered[F](resolver, vendor, name, model)
          attempt
            .recoverWith {
              case LoaderIgluError.SchemaListNotFound(_, error) if isInputError(error) =>
                attempt
            }
            .leftMap { resolutionError =>
              val message = s"Cannot get schemas for iglu:$vendor/$name/jsonschema/$model-*-*  ${resolutionError.asJson.noSpaces}"
              LoaderError.DiscoveryError(DiscoveryFailure.IgluError(message))
            }
            .value
        }

        def fieldNamesFromTypes(types: List[WideRow.Type]): EitherT[F, LoaderIgluError, List[String]] =
          NonAtomicFieldsProvider.fieldsFromTypes(resolver, types).map(_.map(_.field.name))
      }
    }
  }
}
