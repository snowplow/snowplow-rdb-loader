package com.snowplowanalytics.snowplow.rdbloader.shredder.batch

import java.sql.Timestamp
import java.time.Instant

import io.circe.{ACursor, Json}

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer.JsonPointer
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer
import com.snowplowanalytics.iglu.schemaddl.redshift._
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.DdlGenerator
import com.snowplowanalytics.iglu.schemaddl.migrations.{FlatSchema, SchemaList}

import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{types => sparktypes}

object Columnar {

  def flatten(data: Json, source: SchemaList): List[Any] = {
    val schema = getSchema(source)
    FlatSchema.extractProperties(source).map {
      case (pointer, _) =>
        val columnName = FlatSchema.getName(pointer)
        val value = getPath(pointer.forData, data)
        val dataType = schema.get(columnName) match {
          case Some((required, t)) =>
            sparktypes.StructField(columnName, t, !required)
          case None =>
            throw new RuntimeException(s"No $columnName in schema $schema")
        }

        dataType.dataType match {
          case sparktypes.TimestampType =>
            value.asString match {
              case Some(s) => Timestamp.from(Instant.parse(s))
              case None if dataType.nullable && value.isNull => null
              case None => throw new RuntimeException(s"Unepxected value ${value.noSpaces} with in ${dataType}")
            }
          case sparktypes.DateType =>
            value.asString match {
              case Some(s) => java.sql.Date.valueOf(s)
              case None if dataType.nullable && value.isNull => null
              case None => throw new RuntimeException(s"Unepxected value ${value.noSpaces} with in ${dataType}")
            }
          case sparktypes.IntegerType =>
            value.asNumber.flatMap(_.toInt) match {
              case Some(i) => i
              case None if dataType.nullable && value.isNull => null
              case None => throw new RuntimeException(s"Unepxected value ${value.noSpaces} with in ${dataType}")
            }
          case sparktypes.LongType =>
            value.asNumber.flatMap(_.toLong) match {
              case Some(l) => l
              case None if dataType.nullable && value.isNull => null
              case None => throw new RuntimeException(s"Unepxected value ${value.noSpaces} with in ${dataType}")
            }
          case sparktypes.DoubleType =>
            value.asNumber.map(_.toDouble) match {
              case Some(l) => l
              case None if dataType.nullable && value.isNull => null
              case None => throw new RuntimeException(s"Unepxected value ${value.noSpaces} with in ${dataType}")
            }
          case sparktypes.BooleanType =>
            value.asBoolean match {
              case Some(b) => b
              case None if dataType.nullable && value.isNull => null
              case None => throw new RuntimeException(s"Unepxected value ${value.noSpaces} with in ${dataType}")
            }
          case sparktypes.StringType =>
            if (dataType.nullable && value.isNull) null else value.noSpaces
        }

    }
  }

  /**
   * @param source state of schema, providing proper order
   */
  def getSparkSchema(source: SchemaList): sparktypes.StructType = {
    val properties = FlatSchema.extractProperties(source)
    val schema = getSchema(source)
    val metaFields = List(
      StructField("vendor", StringType, false),
      StructField("name", StringType, false),
      StructField("format", StringType, false),
      StructField("model", StringType, false)
    )
    val sparkFields = properties.map {
      case (pointer, _) =>
        val columnName = FlatSchema.getName(pointer)
        schema.get(columnName) match {
          case Some((required, t)) =>
            sparktypes.StructField(columnName, t, !required)
          case None =>
            throw new RuntimeException(s"No $columnName in schema $schema")
        }
    }
    sparktypes.StructType(metaFields ++ sparkFields)
  }


  def getPath(pointer: JsonPointer, json: Json): Json = {
    def go(cursor: List[Pointer.Cursor], data: ACursor): Json =
      cursor match {
        case Nil =>
          data.focus.getOrElse(Json.Null)
        case Pointer.Cursor.DownField(field) :: t =>
          go(t, data.downField(field))
        case Pointer.Cursor.At(i) :: t =>
          go(t, data.downN(i))
        case Pointer.Cursor.DownProperty(_) :: _ =>
          throw new IllegalStateException(s"Iglu Schema DDL tried to use invalid pointer ${pointer.show} for payload ${json.noSpaces}")
      }

    go(pointer.get, json.hcursor)
  }

  /** Redshift-compatible schema */
  def getSchema(source: SchemaList): Map[String, (Boolean, sparktypes.DataType)] = {
    val properties = FlatSchema.extractProperties(source)
    DdlGenerator
      .generateTableDdl(properties, "", None, 1024, false)
      .columns
      .map(e => (e.columnName, e.columnConstraints.contains(Nullability(NotNull)), e.dataType)).map {
      case (n, r, RedshiftTimestamp) => (n, (r, sparktypes.TimestampType))
      case (n, r, RedshiftDate) => (n, (r, sparktypes.DateType))
      case (n, r, RedshiftInteger | RedshiftSmallInt) => (n, (r, sparktypes.IntegerType))
      case (n, r, RedshiftBigInt) => (n, (r, sparktypes.LongType))
      case (n, r, RedshiftReal | RedshiftDecimal(_, _) | RedshiftDouble) => (n, (r, sparktypes.DoubleType))
      case (n, r, RedshiftBoolean) => (n, (r, sparktypes.BooleanType))
      case (n, r, RedshiftVarchar(_) | RedshiftChar(_)) => (n, (r, sparktypes.StringType))
      case (n, r, ProductType(_, _)) =>  (n, (r, sparktypes.StringType))
    }.toMap
  }
}
