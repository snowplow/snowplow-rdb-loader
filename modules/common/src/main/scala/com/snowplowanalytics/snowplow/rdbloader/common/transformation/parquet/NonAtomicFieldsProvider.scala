package com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet

import cats.Monad
import cats.data.EitherT
import cats.effect.Clock
import cats.implicits.toTraverseOps
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent
import com.snowplowanalytics.snowplow.badrows.FailureDetails.LoaderIgluError
import com.snowplowanalytics.iglu.schemaddl.parquet.Migrations.isSchemaMigrationBreaking
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields.{NonAtomicFields, TypedField}
import com.snowplowanalytics.snowplow.rdbloader.common.SchemaProvider._
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, SchemaProvider}

import scala.math.Ordered.orderingToOrdered
import scala.math.abs

object NonAtomicFieldsProvider {
  

  def build[F[_]: Clock: Monad: RegistryLookup](
    resolver: Resolver[F],
    types: List[WideRow.Type]): EitherT[F, LoaderIgluError, NonAtomicFields] = {
  types
    .sorted
    // collapse subversions of the same schema. This will avoid listing the earlier schema versions multiple times.
    .foldRight(List.empty[WideRow.Type])((it, acc) => acc match {
      case Nil => List(it)
      case head::_ => 
        if (head.schemaKey.vendor ==  it.schemaKey.vendor &
            head.schemaKey.name ==  it.schemaKey.name & 
            head.schemaKey.format ==  it.schemaKey.format &
            head.schemaKey.version.model ==  it.schemaKey.version.model)
          acc
        else
          it::acc
    })
    .flatTraverse(fieldFromType(resolver))
    .map(NonAtomicFields)
  }

  private def extractEndSchemas(`type`: WideRow.Type)(schemas: List[SchemaWithKey]):  List[TypedField] = {

    case class FieldWithKey(schemaKey: SchemaKey, field: Field){
      def isBreaking(other: FieldWithKey): Boolean = isSchemaMigrationBreaking(other.field, field)

      def toTypedField(lowerBound: Option[TypedField]): TypedField =
        TypedField(field = field, `type` = `type`.copy(
          schemaKey = schemaKey
        ), lowerExclSchemaBound = lowerBound.map(_.`type`.schemaKey))
    }

    // Schemas need to be ordered to detect schema changes.
    schemas.sorted.reverse
      // Create fields, since the schema-ddl's `isSchemaMigrationBreaking` is operating on the `Field` rather then
      // schemas. Schema keys must be preserved to filter out the Event schemas later.
      // Descending order was required to preserve latest schema in the first list item.
      .map(schemaWithKey => FieldWithKey(schemaWithKey.schemaKey, fieldFromSchema(`type`)(schemaWithKey.schema)))
      // drop the non breaking schemas from the list
      .foldLeft(List.empty[FieldWithKey])(
        (prevSchemaKeyList, fieldWithKey) => prevSchemaKeyList match {
          case Nil => List(fieldWithKey)
          case head::_ if head isBreaking fieldWithKey => fieldWithKey::prevSchemaKeyList
          case _ => prevSchemaKeyList
        }
        )
      // Build `TypedField` from `FieldWithKey`, using next element in list as lower bound. Because list had descending
      // sort.
      .foldLeft(List.empty[TypedField])(
        (prevSchemaKeyList, fieldWithKey) => prevSchemaKeyList match {
          case Nil => fieldWithKey.toTypedField(None)::prevSchemaKeyList
          case head::_ => fieldWithKey.toTypedField(Some(head))
            .copy(field = {
              // hash ensures that loading does not break if user creates a new broken version of the same SchemaKey
              val hash = abs(fieldWithKey.field.hashCode())
              val recoverPoint = fieldWithKey.schemaKey.version.asString.replaceAll("-", "_")
              val newName = s"${fieldWithKey.field.name}_recovered_${recoverPoint}_$hash"
              fieldWithKey.field.copy(name = newName)
            })::prevSchemaKeyList
        }
      )
  }
  
  private def fieldFromType[F[_]: Clock: Monad: RegistryLookup](
     resolver: Resolver[F])(
    `type`: WideRow.Type): EitherT[F, LoaderIgluError, List[TypedField]] =
  SchemaProvider
    .fetchSchemasWithSameModel[F](resolver, `type`.schemaKey)
    // Preserve the historic behaviour by dropping the schemas newer then max in this batch
    .map(listOfSchemas => listOfSchemas.filter(_.schemaKey <= `type`.schemaKey))
    .map(extractEndSchemas(`type`))
    
  private def fieldFromSchema(`type`: WideRow.Type)(schema: Schema): Field = {
    val fieldName = SnowplowEvent.transformSchema(`type`.snowplowEntity.toSdkProperty, `type`.schemaKey)

    `type`.snowplowEntity match {
      case LoaderMessage.SnowplowEntity.SelfDescribingEvent =>
        Field.build(fieldName, schema, enforceValuePresence = false)
      case LoaderMessage.SnowplowEntity.Context =>
        Field.buildRepeated(fieldName, schema, enforceItemPresence = true, Type.Nullability.Nullable)
    }
  }
}
