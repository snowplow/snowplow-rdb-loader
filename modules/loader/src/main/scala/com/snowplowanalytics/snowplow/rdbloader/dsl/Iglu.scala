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

import cats.data.{EitherT, NonEmptyList}
import cats.~>
import cats.implicits._
import cats.effect.{Async, Resource}
import io.circe.Json
import io.circe.syntax._
import org.http4s.client.Client
import com.snowplowanalytics.iglu.client.resolver.{Resolver, ResolverCache}
import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, RegistryLookup}
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.IgluSchema
import com.snowplowanalytics.snowplow.badrows.FailureDetails.LoaderIgluError
import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow
import com.snowplowanalytics.snowplow.rdbloader.common._
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.NonAtomicFieldsProvider
import com.snowplowanalytics.snowplow.rdbloader.common.SchemaProvider.SchemaWithKey
import com.snowplowanalytics.snowplow.rdbloader.discovery.DiscoveryFailure

trait Iglu[F[_]] { self =>

  /**
   * Retrieve list of schemas which belongs to the same vendor-name-model combination with the given
   * schema key
   */
  def getSchemasWithSameModel(
    schemaKey: SchemaKey
  ): F[Either[LoaderError, NonEmptyList[IgluSchema]]]

  def fieldNamesFromTypes(types: List[WideRow.Type]): EitherT[F, LoaderIgluError, List[String]]

  def mapK[G[_]](arrow: F ~> G): Iglu[G] = new Iglu[G] {

    override def getSchemasWithSameModel(
      schemaKey: SchemaKey
    ): G[Either[LoaderError, NonEmptyList[IgluSchema]]] = arrow(self.getSchemasWithSameModel(schemaKey))

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
        override def getSchemasWithSameModel(
          schemaKey: SchemaKey
        ): F[Either[LoaderError, NonEmptyList[IgluSchema]]] =
          SchemaProvider
            .fetchSchemasWithSameModel(resolver, schemaKey)
            .leftMap(igluErr => LoaderError.DiscoveryError(DiscoveryFailure.IgluError(igluErr.asJson.noSpaces)))
            .flatMap { schemaWithKeyList =>
              EitherT
                .fromOption[F][LoaderError, NonEmptyList[SchemaWithKey]](
                  NonEmptyList.fromList(schemaWithKeyList),
                  LoaderError.DiscoveryError(
                    DiscoveryFailure.IgluError(
                      s"Empty response from resolver for ${schemaKey.toSchemaUri}"
                    )
                  )
                )
                .map(_.map(swk => SelfDescribingSchema[Schema](SchemaMap(swk.schemaKey), swk.schema)))
            }
            .value

        def fieldNamesFromTypes(types: List[WideRow.Type]): EitherT[F, LoaderIgluError, List[String]] =
          NonAtomicFieldsProvider.fieldsFromTypes(resolver, types).map(_.map(_.field.name))
      }
    }
  }
}
