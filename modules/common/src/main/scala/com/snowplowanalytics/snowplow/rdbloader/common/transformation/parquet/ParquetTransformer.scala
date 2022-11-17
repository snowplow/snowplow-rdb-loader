package com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet

import cats.data.NonEmptyList
import cats.syntax.all._
import com.snowplowanalytics.snowplow.rdbloader.common.SchemaProvider._
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.schemaddl.parquet.{CastError, Field, FieldValue}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, FailureDetails, Payload, Processor}
import com.snowplowanalytics.snowplow.rdbloader.common.Common.AtomicSchema
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data.ParquetData
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data.ParquetData.FieldWithValue
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields._
import io.circe.Json

import scala.math.Ordered.orderingToOrdered

object ParquetTransformer {

  def transform(
    event: Event,
    allFields: AllFields,
    processor: Processor
  ): Either[BadRow, Transformed.Parquet] =
    for {
      atomicValues <- extractAtomicFieldValues(allFields.atomic, event, processor)
      nonAtomicValues <- extractNonAtomicFieldValues(allFields.nonAtomicFields, processor, event)
    } yield Transformed.Parquet(ParquetData(atomicValues ::: nonAtomicValues))

  private def extractAtomicFieldValues(
    atomicFields: AtomicFields,
    event: Event,
    processor: Processor
  ): Either[BadRow, List[FieldWithValue]] = {

    val atomicJsonValues = event.atomic

    atomicFields.value
      .traverse { atomicField =>
        val jsonFieldValue = atomicJsonValues.getOrElse(atomicField.name, Json.Null)

        provideValue(atomicField, jsonFieldValue)
          .leftMap(castingBadRow(event, processor, AtomicSchema))
      }
  }

  private def extractNonAtomicFieldValues(
    nonAtomicFields: NonAtomicFields,
    processor: Processor,
    event: Event
  ): Either[BadRow, List[FieldWithValue]] =
    nonAtomicFields.value
      .traverse { typedField =>
        valueFromTypedField(typedField, event)
          .leftMap(castingBadRow(event, processor, typedField.`type`.schemaKey))
      }

  private def valueFromTypedField(fieldWithType: TypedField, event: Event): Either[NonEmptyList[CastError], FieldWithValue] =
    fieldWithType.`type`.snowplowEntity match {
      case LoaderMessage.SnowplowEntity.SelfDescribingEvent =>
        forUnstruct(fieldWithType, event)
      case LoaderMessage.SnowplowEntity.Context =>
        forContexts(fieldWithType, event)
    }

  private def forUnstruct(typedField: TypedField, event: Event): Either[NonEmptyList[CastError], FieldWithValue] =
    event.unstruct_event.data match {
      case Some(SelfDescribingData(schemaKey, unstructData)) if isSchemaKeyInField(schemaKey, typedField) =>
        provideValue(typedField.field, unstructData)
      case _ =>
        Right(FieldWithValue(typedField.field, FieldValue.NullValue))
    }

  private def forContexts(typedField: TypedField, event: Event): Either[NonEmptyList[CastError], FieldWithValue] = {
    val allContexts = event.contexts.data ::: event.derived_contexts.data
    val matchingContexts = allContexts
      .filter(context => isSchemaKeyInField(context.schema, typedField))

    if (matchingContexts.nonEmpty) {
      val jsonArrayWithContexts = Json.fromValues(matchingContexts.map(_.data).toVector)
      provideValue(typedField.field, jsonArrayWithContexts)
    } else {
      Right(FieldWithValue(typedField.field, FieldValue.NullValue))
    }
  }

  private def provideValue(field: Field, jsonValue: Json): Either[NonEmptyList[CastError], FieldWithValue] =
    FieldValue
      .cast(field)(jsonValue)
      .toEither
      .map(value => FieldWithValue(field, value))

  private def isSchemaKeyInField(schemaKey: SchemaKey, typedField: TypedField): Boolean =
    schemaKey.vendor === typedField.`type`.schemaKey.vendor &
      schemaKey.name === typedField.`type`.schemaKey.name &
      schemaKey.version.model === typedField.`type`.schemaKey.version.model &
      (schemaKey <= typedField.`type`.schemaKey) &
      typedField.lowerExclSchemaBound.forall(schemaKey > _)

  private def castingBadRow(
    event: Event,
    processor: Processor,
    schemaKey: SchemaKey
  )(
    error: NonEmptyList[CastError]
  ): BadRow = {
    val loaderIgluErrors = error.map(castErrorToLoaderIgluError(schemaKey))
    igluBadRow(event, processor, loaderIgluErrors)
  }

  private def castErrorToLoaderIgluError(schemaKey: SchemaKey)(castError: CastError): FailureDetails.LoaderIgluError =
    castError match {
      case CastError.WrongType(v, e) => FailureDetails.LoaderIgluError.WrongType(schemaKey, v, e.toString)
      case CastError.MissingInValue(k, v) => FailureDetails.LoaderIgluError.MissingInValue(schemaKey, k, v)
    }

  private def igluBadRow(
    event: Event,
    processor: Processor,
    errors: NonEmptyList[FailureDetails.LoaderIgluError]
  ): BadRow = {
    val failure = Failure.LoaderIgluErrors(errors)
    val payload = Payload.LoaderPayload(event)
    BadRow.LoaderIgluError(processor, failure, payload)
  }
}
