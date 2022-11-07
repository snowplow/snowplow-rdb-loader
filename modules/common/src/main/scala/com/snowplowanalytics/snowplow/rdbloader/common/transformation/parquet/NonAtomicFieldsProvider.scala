package com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet

import cats.Monad
import cats.data.EitherT
import cats.effect.Clock
import cats.implicits.toTraverseOps
import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverResult
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.client.{ClientError, Resolver}
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits.toSchema
import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent
import com.snowplowanalytics.snowplow.badrows.FailureDetails
import com.snowplowanalytics.snowplow.badrows.FailureDetails.LoaderIgluError
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow.latestByModel
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.ParquetFieldCache
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields.{NonAtomicFields, TypedField}
import io.circe.Json

object NonAtomicFieldsProvider {

  def build[F[_]: Clock: Monad: RegistryLookup](
    resolver: Resolver[F],
    cache: ParquetFieldCache[F],
    types: List[WideRow.Type]
  ): EitherT[F, LoaderIgluError, NonAtomicFields] =
    latestByModel(types).sorted
      .traverse(`type` => createTypedField(resolver, cache, `type`))
      .map(NonAtomicFields)

  private def createTypedField[F[_]: Clock: Monad: RegistryLookup](
    resolver: Resolver[F],
    cache: ParquetFieldCache[F],
    `type`: WideRow.Type
  ): EitherT[F, LoaderIgluError, TypedField] =
    getField(resolver, cache, `type`)
      .map(field => TypedField(field, `type`))

  private def getField[F[_]: Clock: Monad: RegistryLookup](
    resolver: Resolver[F],
    cache: ParquetFieldCache[F],
    `type`: WideRow.Type
  ): EitherT[F, FailureDetails.LoaderIgluError, Field] =
    EitherT(resolver.lookupSchemaResult(`type`.schemaKey))
      .leftMap(resolverBadRow(`type`.schemaKey))
      .flatMap {
        case resolverResult: ResolverResult.Cached[SchemaKey, Json] =>
          lookupInCache(cache, resolverResult, `type`)
        case ResolverResult.NotCached(value) =>
          EitherT.fromEither(evaluateField(value, `type`))
      }

  private def lookupInCache[F[_]: Monad](
    cache: ParquetFieldCache[F],
    resolverResult: ResolverResult.Cached[SchemaKey, Json],
    `type`: WideRow.Type
  ): EitherT[F, FailureDetails.LoaderIgluError, Field] = {
    val key = (resolverResult.key, resolverResult.timestamp)

    EitherT.liftF(cache.get(key)).flatMap {
      case Some(field) =>
        EitherT.pure[F, FailureDetails.LoaderIgluError](field)
      case None =>
        EitherT
          .fromEither(evaluateField(resolverResult.value, `type`))
          .semiflatTap(field => cache.put(key, field))
    }
  }

  private def evaluateField(json: Json, `type`: WideRow.Type): Either[FailureDetails.LoaderIgluError, Field] =
    Schema
      .parse(json)
      .map(schema => fieldFromSchema(schema, `type`))
      .toRight(parseSchemaBadRow(`type`.schemaKey))

  private def resolverBadRow(schemaKey: SchemaKey)(e: ClientError.ResolutionError): FailureDetails.LoaderIgluError =
    FailureDetails.LoaderIgluError.IgluError(schemaKey, e)

  private def parseSchemaBadRow(schemaKey: SchemaKey): FailureDetails.LoaderIgluError =
    FailureDetails.LoaderIgluError.InvalidSchema(schemaKey, "Cannot be parsed as JSON Schema AST")

  private def fieldFromSchema(schema: Schema, `type`: WideRow.Type): Field = {
    val fieldName = SnowplowEvent.transformSchema(`type`.snowplowEntity.toSdkProperty, `type`.schemaKey)

    `type`.snowplowEntity match {
      case LoaderMessage.SnowplowEntity.SelfDescribingEvent =>
        Field.build(fieldName, schema, enforceValuePresence = false)
      case LoaderMessage.SnowplowEntity.Context =>
        Field.buildRepeated(fieldName, schema, enforceItemPresence = true, Type.Nullability.Nullable)
    }
  }
}
