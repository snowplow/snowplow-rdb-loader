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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.parquet

import com.github.mjakubowski84.parquet4s.SchemaDef
import com.snowplowanalytics.iglu.schemaddl.parquet.Type.DecimalPrecision
import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields.AllFields
import org.apache.parquet.schema.LogicalTypeAnnotation._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.{LogicalTypeAnnotation, MessageType, Type => PType, Types}

private[parquet] object ParquetSchema {

  val byteArrayLength = 16 // cause 16 bytes is required by max precision 38

  def build(allFields: AllFields): MessageType = {
    val fieldsOnly = allFields.fieldsOnly
    val types      = fieldsOnly.map(asParquetField)
    Types
      .buildMessage()
      .addFields(types: _*)
      .named("snowplow")
  }

  private def asParquetField(field: Field): PType = {
    val normalizedName = Field.normalize(field).name

    asParquetType(field.fieldType)
      .withRequired(field.nullability.required)
      .apply(normalizedName)
  }

  private def asParquetType(fieldType: Type): SchemaDef = fieldType match {
    case Type.String =>
      SchemaDef.primitive(BINARY, logicalTypeAnnotation = Some(stringType()))
    case Type.Json =>
      SchemaDef.primitive(BINARY, logicalTypeAnnotation = Some(jsonType()))
    case Type.Boolean =>
      SchemaDef.primitive(BOOLEAN)
    case Type.Integer =>
      SchemaDef.primitive(INT32)
    case Type.Long =>
      SchemaDef.primitive(INT64)
    case Type.Double =>
      SchemaDef.primitive(DOUBLE)
    case Type.Decimal(precision, scale) =>
      val logicalType = Option(LogicalTypeAnnotation.decimalType(scale, Type.DecimalPrecision.toInt(precision)))
      precision match {
        case DecimalPrecision.Digits9 =>
          SchemaDef.primitive(INT32, logicalTypeAnnotation = logicalType)
        case DecimalPrecision.Digits18 =>
          SchemaDef.primitive(INT64, logicalTypeAnnotation = logicalType)
        case DecimalPrecision.Digits38 =>
          SchemaDef.primitive(FIXED_LEN_BYTE_ARRAY, logicalTypeAnnotation = logicalType, length = Option(byteArrayLength))
      }
    case Type.Date =>
      SchemaDef.primitive(INT32, logicalTypeAnnotation = Some(dateType()))
    case Type.Timestamp =>
      SchemaDef.primitive(INT64, logicalTypeAnnotation = Some(timestampType(true, TimeUnit.MICROS)))
    case Type.Array(element, elementNullability) =>
      val listElement = asParquetType(element).withRequired(elementNullability.required)
      SchemaDef.list(listElement)
    case Type.Struct(subFields) =>
      SchemaDef.group(subFields.map(asParquetField).toVector: _*)
  }
}
