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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields.AllFields
import org.apache.spark.sql.types._

object SparkSchema {

  def build(allFields: AllFields): StructType =
    StructType {
      allFields.fieldsOnly
        .map(asSparkField)
    }

  private def asSparkField(ddlField: Field): StructField = {
    val normalizedName = Field.normalize(ddlField).name
    val dataType       = fieldType(ddlField.fieldType)
    StructField(normalizedName, dataType, ddlField.nullability.nullable)
  }

  private def fieldType(ddlType: Type): DataType = ddlType match {
    case Type.String                        => StringType
    case Type.Boolean                       => BooleanType
    case Type.Integer                       => IntegerType
    case Type.Long                          => LongType
    case Type.Double                        => DoubleType
    case Type.Decimal(precision, scale)     => DecimalType(Type.DecimalPrecision.toInt(precision), scale)
    case Type.Date                          => DateType
    case Type.Timestamp                     => TimestampType
    case Type.Struct(fields)                => StructType(fields.map(asSparkField))
    case Type.Array(element, elNullability) => ArrayType(fieldType(element), elNullability.nullable)
    case Type.Json                          => StringType // Spark does not support the `Json` parquet logical type.
  }

}
