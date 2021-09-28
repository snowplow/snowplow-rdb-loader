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
package com.snowplowanalytics.snowplow.rdbloader.shredder.batch

import java.time.Instant

import cats.Id
import cats.data.EitherT

import io.circe.{ACursor, Json}

import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{types => sparktypes}

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer
import com.snowplowanalytics.iglu.schemaddl.redshift._
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.DdlGenerator
import com.snowplowanalytics.iglu.schemaddl.migrations.{FlatSchema, SchemaList}

import com.snowplowanalytics.iglu.core.SelfDescribingData

import com.snowplowanalytics.iglu.client.Resolver

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.rdbloader.common.catsClockIdInstance
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{ Hierarchy, Shredded, Flattening }

object Columnar {

  def flatten(data: Json, source: SchemaList): List[Any] = {
    val schema = getSchema(source)
    FlatSchema.extractProperties(source).map {      // TODO: get the schema from cache
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
              case Some(s) => Instant.parse(s)
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
   * Get Spark schema for a particular schema (represented by a final `SchemaList`)
   * along with meta fields (vendor, name, model)
   * @param source state of schema, providing proper order
   */
  def getSparkSchema(source: SchemaList): sparktypes.StructType = {
    val properties = FlatSchema.extractProperties(source)
    val schema = getSchema(source)
    val metaFields = List(
      StructField("schema_vendor", StringType, false),
      StructField("schema_name", StringType, false),
      StructField("schema_format", StringType, false),
      StructField("schema_version", StringType, false),
      StructField("root_id", StringType, false),
      StructField("root_tstamp", sparktypes.TimestampType, false)   // Will be casted into timestamp at write-site
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

  def getPath(pointer: Pointer.JsonPointer, json: Json): Json = {
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

  def flattenAny(resolver: Resolver[Id], instance: SelfDescribingData[Json]): EitherT[Id, FailureDetails.LoaderIgluError, List[Any]] =
    Flattening.getOrdered(resolver, instance.schema).map { ordered => flatten(instance.data, ordered) }

  def shred(resolver: Resolver[Id])(hierarchy: Hierarchy): EitherT[Id, FailureDetails.LoaderIgluError, Shredded] = {
    Flattening
      .getOrdered(resolver, hierarchy.entity.schema)
      .map { ordered => flatten(hierarchy.entity.data, ordered) }
      .map { columns =>
        val schema = hierarchy.entity.schema
        val data: List[Any] = List(schema.vendor, schema.name, schema.format, schema.version.asString, hierarchy.eventId.toString, hierarchy.collectorTstamp) ::: columns
        Shredded.Parquet(schema.vendor, schema.name, schema.version.model, data)
      }
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
