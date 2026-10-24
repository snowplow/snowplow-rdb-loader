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
package com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet

import cats.Monad
import cats.data.{EitherT, NonEmptyList}
import cats.effect.Clock
import cats.syntax.all._
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.iglu.schemaddl.parquet.Migrations.mergeSchemas
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent
import com.snowplowanalytics.snowplow.badrows.FailureDetails
import com.snowplowanalytics.snowplow.badrows.FailureDetails.LoaderIgluError
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.SnowplowEntity
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields.{NonAtomicFields, TypedField}
import com.snowplowanalytics.snowplow.rdbloader.common.SchemaProvider._
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, SchemaProvider}
import io.circe.Json
import io.circe.syntax._

import scala.math.Ordered.orderingToOrdered
import scala.math.abs

object NonAtomicFieldsProvider {

  /** Which parquet column each schema key's data was written to. See [[columnNamesByType]]. */
  type ColumnNamesByType = Map[(SchemaKey, SnowplowEntity), String]

  /** How the hash at the end of a recovered column's name is computed. */
  sealed trait RecoveryHash {
    def apply(field: Field, schema: Schema): String
  }

  object RecoveryHash {

    /**
     * Hash of a canonical serialization of the recovered version's JSON Schema, with object keys
     * and array elements sorted recursively. schema-ddl backs `properties`, `patternProperties` and
     * `type` unions with Scala `Map`s and `Set`s, whose iteration order differs between 2.12 and
     * 2.13 from 5 entries, so plain `asJson` would give different names on each.
     */
    case object CanonicalSchemaJson extends RecoveryHash {
      def apply(field: Field, schema: Schema): String = "%08x".format(canonical(schema.asJson).noSpaces.hashCode())

      private def canonical(json: Json): Json =
        json.arrayOrObject(
          json,
          values => Json.fromValues(values.map(canonical).sortBy(_.noSpaces)),
          obj => Json.fromFields(obj.toList.map { case (k, v) => k -> canonical(v) }.sortBy(_._1))
        )
    }

    /**
     * The JVM case-class hash of the parquet field, used by transformers before the switch to
     * [[CanonicalSchemaJson]]. Kept only for the loader's pre-`2-0-2` fallback. It matches parquet
     * written by Scala 2.12 transformers only while the loader itself is on 2.12. Delete it when
     * the loader moves to 2.13.
     */
    case object LegacyFieldHashCode extends RecoveryHash {
      def apply(field: Field, schema: Schema): String = abs(field.hashCode()).toString
    }
  }

  /**
   * Builds a `NonAtomicFields`.
   * @param resolver
   *   Iglu resolver, that would be used to fetch the schema list and their content for the `types`.
   * @param types
   *   list of schemas types (SchemaKey and Struct/Unstruct flag) for the single event
   * @tparam F
   *   \- IO
   * @return
   *   List of unique TypedFields. See `extractEndSchemas` docstring for explanation of TypedFields.
   */
  def build[F[_]: Clock: Monad: RegistryLookup](
    resolver: Resolver[F],
    types: List[WideRow.Type]
  ): EitherT[F, LoaderIgluError, NonAtomicFields] =
    fieldsFromTypes(resolver, types, RecoveryHash.CanonicalSchemaJson).map(NonAtomicFields.apply)

  /**
   * Extract TypedFields for the types of the same family in event. It could produce multiple
   * TypedFields. If output is longer than one item, it means that user created a broken migration.
   * Broken migration constitutes a illegal type casting and defined in schema-dll package accessed
   * by `mergeSchema`.
   *
   * For example, changing field type from integer to string between 1-0-0 and 1-0-1 versions. That
   * example, would produce two Fields (in pseudocode):
   *
   *   - TypedField( * field ("my_iglu_schema_1", type={"field": INT64}), * type contexts *
   *     matchingKeys = List(1-0-0) )
   *   - TypedField( * field ("my_iglu_schema_1_recovered_1_0_1_<hash>", type={"field": STRING}), *
   *     type contexts * matchingKeys = List(1-0-1)
   *
   * Algorithm will eagerly merge all events into the base `my_iglu_schema_1` column, creating the
   * `recovered` column per each divergent schema.
   *
   * Where `<hash>` is computed by `recoveryHash`; for transformers, 8 hex characters hashing the
   * canonical 1-0-1 JSON Schema.
   *
   * @param types
   *   All schema key and entity(one of unstuct/contexts) in event for the same family
   * @param schemas
   *   List of schemas with the same vendor/name/format/model as the one provided in `type`
   * @return
   *   TypedFields, which contains fields for casting, and schema key list for matching
   */
  private def extractEndSchemas(
    types: Set[WideRow.Type],
    recoveryHash: RecoveryHash
  )(
    schemas: NonEmptyList[SchemaWithKey]
  ): List[TypedField] = {

    val schemasSorted = schemas.sorted

    // Schemas need to be ordered by key to merge in correct order.
    schemasSorted.toList
      // `types` are all of the same family, so it does not matter which element is passed to fieldFromSchema
      .flatMap { schemaWithKey =>
        fieldFromSchema(types.head)(schemaWithKey.schema)
          .map(field => (TypedField(field, types.head, Set(schemaWithKey.schemaKey)), schemaWithKey.schema))
      }
      // Accumulating vector would contain base column as first element and broken migrations in others
      .foldLeft(Vector.empty[TypedField]) { case (endFields, (typedField, schema)) =>
        endFields.headOption match {
          case Some(rootField) =>
            mergeSchemas(rootField.field, typedField.field) match {
              // Failed merge Left contains the reason, which could be discarded
              case Left(_) =>
                val hash = recoveryHash(typedField.field, schema)
                // typedField always has a single element in matchingKeys
                val recoverPoint = typedField.matchingKeys.head.version.asString.replaceAll("-", "_")
                val newName      = s"${typedField.field.name}_recovered_${recoverPoint}_$hash"
                // broken migrations go to the end of Vector
                if (types.map(_.schemaKey).contains(typedField.matchingKeys.head))
                  endFields :+ typedField.copy(field = typedField.field.copy(name = newName))
                else
                  // do not create a recovered column if that type were not in the event
                  endFields
              case Right(mergedField) =>
                // keep on updating first element (base schema) in the vector with the merged schemas
                endFields.updated(0, TypedField(mergedField, types.max, rootField.matchingKeys ++ typedField.matchingKeys))
            }
          case None => Vector(typedField)
        }
      }
      .toList
  }

  def fieldsFromTypes[F[_]: Clock: Monad: RegistryLookup](
    resolver: Resolver[F],
    types: List[WideRow.Type],
    recoveryHash: RecoveryHash
  ): EitherT[F, LoaderIgluError, List[TypedField]] =
    types
      .groupBy(it => (it.snowplowEntity, it.schemaKey.vendor, it.schemaKey.name, it.schemaKey.format, it.schemaKey.version.model))
      .toList
      .sortBy { case (_, vals) => vals.head }
      .map(_._2.toSet)
      .flatTraverse(typeSet =>
        SchemaProvider
          .fetchSchemasWithSameModel[F](resolver, typeSet.max.schemaKey)
          // Preserve the historic behaviour by dropping the schemas newer then max in this batch
          .map(listOfSchemas => listOfSchemas.filter(_.schemaKey <= typeSet.max.schemaKey))
          .flatMap(schemaList =>
            EitherT
              .fromOption[F][LoaderIgluError, NonEmptyList[SchemaWithKey]](
                NonEmptyList.fromList(schemaList),
                FailureDetails.LoaderIgluError.InvalidSchema(
                  typeSet.max.schemaKey,
                  "Iglu list did not return the schemaKey that was requested. Possible reason would be multiple iglu" +
                    " servers in resolver with the same schema family."
                )
              )
              .map(extractEndSchemas(typeSet, recoveryHash))
          )
      )

  /**
   * The parquet column each schema key's data was written to. Keyed on the entity as well, because
   * one schema can appear as both a context and a self-describing event.
   *
   * A key absent from this map contributed no data to any column, so from `shredding_complete`
   * `2-0-2` the loader takes a type with no name to mean "nothing to copy" and skips it. Any change
   * here that stops naming a column which does hold data would therefore drop that column from the
   * load, silently -- see [[com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.Parsed]].
   */
  def columnNamesByType(fields: List[TypedField]): ColumnNamesByType =
    fields.flatMap { typedField =>
      typedField.matchingKeys.map(key => (key, typedField.`type`.snowplowEntity) -> typedField.field.name)
    }.toMap

  private def fieldFromSchema(`type`: WideRow.Type)(schema: Schema): Option[Field] = {
    val fieldName = SnowplowEvent.transformSchema(`type`.snowplowEntity.toSdkProperty, `type`.schemaKey)

    (`type`.snowplowEntity match {
      case LoaderMessage.SnowplowEntity.SelfDescribingEvent =>
        Field.build(fieldName, schema, enforceValuePresence = false)
      case LoaderMessage.SnowplowEntity.Context =>
        Field.buildRepeated(fieldName, schema, enforceItemPresence = true, Type.Nullability.Nullable)
    }).map(Field.normalize)
  }
}
