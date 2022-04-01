package com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet

import cats.Monad
import cats.data.EitherT
import cats.effect.Clock
import cats.implicits.toTraverseOps
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow.latestByModel
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields.{NonAtomicFields, TypedField}
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, SchemaProvider}

object NonAtomicFieldsProvider {

  def build[F[_]: Clock: Monad: RegistryLookup](resolver: Resolver[F],
                                                types: List[WideRow.Type]): F[Either[String, NonAtomicFields]] = {
    latestByModel(types)
      .sorted
      .traverse(createTypedField(resolver))
      .map(NonAtomicFields)
      .value
  }

  private def createTypedField[F[_]: Clock: Monad: RegistryLookup](resolver: Resolver[F])
                                                                  (`type`: WideRow.Type): EitherT[F, String, TypedField] = {
    fieldFromType[F](resolver, `type`)
      .map(field => TypedField(field, `type`))
  }

  private def fieldFromType[F[_]: Clock: Monad: RegistryLookup](resolver: Resolver[F],
                                                                `type`: WideRow.Type): EitherT[F, String, Field] = {
    SchemaProvider
      .getSchema[F](resolver, `type`.schemaKey)
      .map(fieldFromSchema(`type`))
      .leftMap(_.toString)
  }

  private def fieldFromSchema(`type`: WideRow.Type)
                             (schema: Schema): Field = {
    val fieldName = SnowplowEvent.transformSchema(`type`.snowplowEntity.toSdkProperty, `type`.schemaKey)

    `type`.snowplowEntity match {
      case LoaderMessage.SnowplowEntity.SelfDescribingEvent =>
        Field.build(fieldName, schema, enforceValuePresence = false)
      case LoaderMessage.SnowplowEntity.Context =>
        Field.buildRepeated(fieldName, schema, enforceItemPresence = true, Type.Nullability.Nullable)
    }
  }
}
