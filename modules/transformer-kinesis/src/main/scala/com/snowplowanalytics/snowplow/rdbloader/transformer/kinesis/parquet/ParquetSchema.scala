/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.parquet

import com.github.mjakubowski84.parquet4s.SchemaDef
import com.snowplowanalytics.iglu.schemaddl.parquet.Type.DecimalPrecision
import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields.AllFields
import org.apache.parquet.schema.LogicalTypeAnnotation._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.{LogicalTypeAnnotation, MessageType, Types, Type => PType}

private [parquet] object ParquetSchema {

  val byteArrayLength = 16 //cause 16 bytes is required by max precision 38

  def build(allFields: AllFields): MessageType = {
    val fieldsOnly = allFields.fieldsOnly
    val types = fieldsOnly.map(asParquetField)
   Types.buildMessage()
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
      SchemaDef.group(subFields.map(asParquetField): _*)
  }
}
