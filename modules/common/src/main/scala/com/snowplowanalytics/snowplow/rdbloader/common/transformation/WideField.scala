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
package com.snowplowanalytics.snowplow.rdbloader.common.transformation

import java.time.Instant

import cats.implicits._
import cats.Monad
import cats.data.{EitherT, NonEmptyList, Validated, ValidatedNel}
import cats.effect.Clock

import io.circe.Json

import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.client.{ClientError, Resolver}
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.schemaddl.bigquery.{CastError, Field, Mode, Type}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage

object WideField {

  def forEntity[F[_]: Clock: Monad: RegistryLookup](resolver: Resolver[F], stype: LoaderMessage.ShreddedType, event: Event): EitherT[F, BadRow, FieldValue] =
    stype.shredProperty match {
      case LoaderMessage.ShreddedType.SelfDescribingEvent =>
        forUnstruct(resolver, stype, event)
      case LoaderMessage.ShreddedType.Contexts =>
        forContexts(resolver, stype, event)
    }

  def forUnstruct[F[_]: Clock: Monad: RegistryLookup](resolver: Resolver[F], stype: LoaderMessage.ShreddedType, event: Event): EitherT[F, BadRow, FieldValue] = {
    event.unstruct_event.data match {
      case Some(SelfDescribingData(schemaKey, data)) if keysMatch(schemaKey, stype.schemaKey) =>
        for {
          schema <- getSchema(resolver, schemaKey)
          field = Field.build("NOT_NEEDED", schema, false)
          v <- EitherT.fromEither[F](cast(field, data).toEither).leftMap(castingBadRow)
        } yield v
      case _ => EitherT.rightT(FieldValue.NullValue)
    }
  }

  def forContexts[F[_]: Clock: Monad: RegistryLookup](resolver: Resolver[F], stype: LoaderMessage.ShreddedType, event: Event): EitherT[F, BadRow, FieldValue] = {
    val rows = (event.contexts.data ::: event.derived_contexts.data)
      .filter(sdd => keysMatch(sdd.schema, stype.schemaKey))
    if (rows.nonEmpty) {
      for {
        schema <- getSchema(resolver, stype.schemaKey)
        field = Field.build("NOT_NEEDED", schema, true)
        vs <- EitherT.fromEither[F](rows.traverse(sdd => cast(field, sdd.data)).leftMap(castingBadRow).toEither)
      } yield FieldValue.ArrayValue(vs)
    }
    else EitherT.rightT(FieldValue.NullValue)
  }

  private def keysMatch(k1: SchemaKey, k2: SchemaKey): Boolean =
    k1.vendor === k2.vendor && k1.name === k2.name && k1.version.model === k2.version.model

  type CastResult = ValidatedNel[CastError, FieldValue]

  sealed trait FieldValue
  object FieldValue {
    case object NullValue extends FieldValue
    case class StringValue(value: String) extends FieldValue
    case class BooleanValue(value: Boolean) extends FieldValue
    case class LongValue(value: Long) extends FieldValue // TODO: Should we also support 32 bit integer?
    case class DoubleValue(value: Double) extends FieldValue
    case class TimestampValue(value: java.sql.Timestamp) extends FieldValue
    case class DateValue(value: java.sql.Date) extends FieldValue
    case class ArrayValue(values: List[FieldValue]) extends FieldValue
    case class StructValue(values: List[FieldValue]) extends FieldValue
  }

  def cast(field: Field, value: Json): CastResult =
    field match {
      case Field(_, fieldType, Mode.Repeated) => castRepeated(fieldType, value)
      case Field(_, fieldType, mode) => castValue(fieldType, value).recover(mode)
    }

  def castValue(fieldType: Type, value: Json): CastResult = {
    fieldType match {
      case Type.String if value == Json.Null =>
        value.asString
          .fold(CastError.WrongType(value, fieldType).invalidNel[FieldValue])(FieldValue.StringValue(_).validNel)
      case Type.String =>   // Fallback strategy for union types
        value.asString
          .fold(FieldValue.StringValue(value.noSpaces))(FieldValue.StringValue(_))
          .validNel
      case Type.Boolean =>
        value.asBoolean
          .fold(CastError.WrongType(value, fieldType).invalidNel[FieldValue])(FieldValue.BooleanValue(_).validNel)
      case Type.Integer =>
        value.asNumber
          .flatMap(_.toLong)
          .fold(CastError.WrongType(value, fieldType).invalidNel[FieldValue])(FieldValue.LongValue(_).validNel)
      case Type.Float =>
        value.asNumber
          .map(_.toDouble)
          .fold(CastError.WrongType(value, fieldType).invalidNel[FieldValue])(FieldValue.DoubleValue(_).validNel)
      case Type.Timestamp | Type.DateTime=>
        value.asString
          .flatMap(s => Either.catchNonFatal(java.sql.Timestamp.from(Instant.parse(s))).toOption)
          .fold(CastError.WrongType(value, fieldType).invalidNel[FieldValue])(FieldValue.TimestampValue(_).validNel)
      case Type.Date =>
        value.asString
          .flatMap(s => Either.catchNonFatal(java.sql.Date.valueOf(s)).toOption)
          .fold(CastError.WrongType(value, fieldType).invalidNel[FieldValue])(FieldValue.DateValue(_).validNel)
      case Type.Record(subfields) =>
        value
          .asObject
          .fold(CastError.WrongType(value, fieldType).invalidNel[Map[String, Json]])(_.toMap.validNel)
          .andThen(castObject(subfields))
    }
  }

  private implicit class Recover(val value: CastResult) extends AnyVal {
    /** If cast failed, but value is null and column is nullable - fallback to null */
    def recover(mode: Mode): CastResult = value match {
      case Validated.Invalid(NonEmptyList(e @ CastError.WrongType(Json.Null, _), Nil)) =>
        if (mode == Mode.Nullable) FieldValue.NullValue.validNel else e.invalidNel
      case other => other
    }
  }

  /** Part of `castValue`, mapping JSON object into *ordered* list of `TableRow`s */
  def castObject(subfields: List[Field])(jsonObject: Map[String, Json]): CastResult = {
    val results = subfields.map {
      case Field(name, fieldType, Mode.Repeated) =>
        jsonObject.get(name) match {
          case Some(json) => castRepeated(fieldType, json)
          case None => FieldValue.NullValue.validNel[CastError] // TODO: A little bit dangerous.  BigQuery ddl does not tell us if this field can be nullable.
        }
      case Field(name, fieldType, Mode.Nullable) =>
        jsonObject.get(name) match {
          case Some(value) => castValue(fieldType, value).recover(Mode.Nullable)
          case None => FieldValue.NullValue.validNel
        }
      case Field(name, fieldType, Mode.Required) =>
        jsonObject.get(name) match {
          case Some(value) => castValue(fieldType, value)
          case None => CastError.MissingInValue(name, Json.fromFields(jsonObject)).invalidNel
        }
    }

    results
      .sequence[ValidatedNel[CastError, *], FieldValue]
      .map(FieldValue.StructValue)
  }

  /** Try to cast JSON into a list of `fieldType`, fail if JSON is not an array */
  def castRepeated(fieldType: Type, json: Json): CastResult =
    json.asArray match {
      case Some(values) => values
        .toList
        .map(castValue(fieldType, _))
        .sequence[ValidatedNel[CastError, *], FieldValue]
        .map(FieldValue.ArrayValue.apply)
      case None =>
        json.asNull match {
          case Some(_) => FieldValue.NullValue.validNel  // TODO: A little bit dangerous.  BigQuery ddl does not tell us if this field can be nullable.
          case None => CastError.NotAnArray(json, fieldType).invalidNel
        }
    }

  def getSchema[F[_]: Clock: Monad: RegistryLookup](resolver: Resolver[F], schemaKey: SchemaKey): EitherT[F, BadRow, Schema] =
    for {
      json <- EitherT(resolver.lookupSchema(schemaKey)).leftMap(resolverBadRow)
      schema <- EitherT.fromOption[F](Schema.parse(json), parseSchemaBadRow)
    } yield schema

  def castingBadRow(error: NonEmptyList[CastError]): BadRow = ???
  def resolverBadRow(e: ClientError.ResolutionError): BadRow = ???
  def parseSchemaBadRow: BadRow = ???

}
