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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields.AllFields
import org.apache.spark.sql.types._

object SparkSchema {

  def build(allFields: AllFields): StructType = {
    StructType {
      allFields
        .fieldsOnly
        .map(asSparkField)
    }
  }

  private def asSparkField(ddlField: Field): StructField = {
    val normalizedName = Field.normalize(ddlField).name
    val dataType = fieldType(ddlField.fieldType)
    StructField(normalizedName, dataType, ddlField.nullability.nullable)
  }

  private def fieldType(ddlType: Type): DataType = ddlType match {
    case Type.String => StringType
    case Type.Boolean => BooleanType
    case Type.Integer => IntegerType
    case Type.Long => LongType
    case Type.Double => DoubleType
    case Type.Decimal(precision, scale) => DecimalType(Type.DecimalPrecision.toInt(precision), scale)
    case Type.Date => DateType
    case Type.Timestamp => TimestampType
    case Type.Struct(fields) => StructType(fields.map(asSparkField))
    case Type.Array(element, elNullability) => ArrayType(fieldType(element), elNullability.nullable)
    case Type.Json => StringType // Spark does not support the `Json` parquet logical type.
  }

}
