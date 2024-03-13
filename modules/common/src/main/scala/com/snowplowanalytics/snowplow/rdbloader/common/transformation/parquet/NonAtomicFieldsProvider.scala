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
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.iglu.schemaddl.parquet.Migrations.mergeSchemas
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent
import com.snowplowanalytics.snowplow.badrows.FailureDetails
import com.snowplowanalytics.snowplow.badrows.FailureDetails.LoaderIgluError
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields.{NonAtomicFields, TypedField}
import com.snowplowanalytics.snowplow.rdbloader.common.SchemaProvider._
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, SchemaProvider}

import scala.math.Ordered.orderingToOrdered
import scala.math.abs

object NonAtomicFieldsProvider {

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
  ): EitherT[F, LoaderIgluError, NonAtomicFields] = fieldsFromTypes(resolver, types).map(NonAtomicFields.apply)

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
   *   - TypedField( * field ("my_iglu_schema_1_recovered_1_0_1_9999999", type={"field": STRING}), *
   *     type contexts * matchingKeys = List(1-0-1)
   *
   * Algorithm will eagerly merge all events into the base `my_iglu_schema_1` column, creating the
   * `recovered` column per each divergent schema.
   *
   * Where 9999999 is standard scala (murmur2) hash of the field type.
   *
   * @param types
   *   All schema key and entity(one of unstuct/contexts) in event for the same family
   * @param schemas
   *   List of schemas with the same vendor/name/format/model as the one provided in `type`
   * @return
   *   TypedFields, which contains fields for casting, and schema key list for matching
   */
  private def extractEndSchemas(types: Set[WideRow.Type])(schemas: NonEmptyList[SchemaWithKey]): List[TypedField] = {

    val schemasSorted = schemas.sorted

    // Schemas need to be ordered by key to merge in correct order.
    schemasSorted
      // `types` are all of the same family, so it does not matter which element is passed to fieldFromSchema
      .map(schemaWithKey => TypedField(fieldFromSchema(types.head)(schemaWithKey.schema), types.head, Set(schemaWithKey.schemaKey)))
      // Accumulating vector would contain base column as first element and broken migrations in others
      .foldLeft(Vector.empty[TypedField])((endFields, typedField) =>
        endFields.headOption match {
          case Some(rootField) =>
            mergeSchemas(rootField.field, typedField.field) match {
              // Failed merge Left contains the reason, which could be discarded
              case Left(_) =>
                val hash = abs(typedField.field.hashCode())
                // typedField always has a single element in matchingKeys
                val recoverPoint = typedField.matchingKeys.head.version.asString.replaceAll("-", "_")
                val newName = s"${typedField.field.name}_recovered_${recoverPoint}_$hash"
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
      )
      .toList
  }

  def fieldsFromTypes[F[_]: Clock: Monad: RegistryLookup](
    resolver: Resolver[F],
    types: List[WideRow.Type]
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
              .map(extractEndSchemas(typeSet))
          )
      )

  private def fieldFromSchema(`type`: WideRow.Type)(schema: Schema): Field = {
    val fieldName = SnowplowEvent.transformSchema(`type`.snowplowEntity.toSdkProperty, `type`.schemaKey)

    Field.normalize(`type`.snowplowEntity match {
      case LoaderMessage.SnowplowEntity.SelfDescribingEvent =>
        Field.build(fieldName, schema, enforceValuePresence = false)
      case LoaderMessage.SnowplowEntity.Context =>
        Field.buildRepeated(fieldName, schema, enforceItemPresence = true, Type.Nullability.Nullable)
    })
  }
}
