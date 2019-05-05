/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader

import cats.data.{EitherT, State}
import cats.implicits._

import io.circe.literal._

import com.snowplowanalytics.iglu.core._
import com.snowplowanalytics.iglu.schemaddl.IgluSchema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.iglu.schemaddl.migrations.{ SchemaList => DSchemaList }

import com.snowplowanalytics.snowplow.rdbloader.db.Decoder
import com.snowplowanalytics.snowplow.rdbloader.Security.Tunnel
import com.snowplowanalytics.snowplow.rdbloader.db.Entities.{Columns, TableState}


object TestInterpreter {

  type Test[A] = State[List[String], A]

  def fetch(key: SchemaKey): EitherT[Test, String, IgluSchema] = {
    val state = State[List[String], IgluSchema] { log =>
      val result = Schema.parse(json"""{}""").getOrElse(throw new RuntimeException("Not a valid JSON schema"))
      val schema = SelfDescribingSchema(SchemaMap(key), result)
      (s"Fetch ${key.toSchemaUri}" :: log, schema)
    }
    EitherT.liftF(state)
  }

  def executeUpdate(query: String): Test[Either[LoaderError, Long]] =
    State { log => (trim(query) :: log, 1L.asRight) }

  def print(message: String): Test[Unit] =
    State { log => (message :: log, ()) }

  def executeQuery[A](query: String, decoder: Decoder[A]): Test[Either[LoaderError, A]] = {
    val result = decoder.name match {
      case "TableState" => TableState(SchemaKey("com.acme", "some_context", "jsonschema", SchemaVer.Full(2,0,0)))
      case "Boolean" => false
      case "Columns" => Columns(List("some_column"))
    }
    State { log => (trim(query) :: log, Right(result.asInstanceOf[A])) }
  }

  def getEc2Property(name: String): Test[Either[LoaderError, String]] =
    State { log =>
      val value = "EC2 PROPERTY " ++ name ++ " key"
      (value :: log, Right(value))
    }

  def establishTunnel(tunnelConfig: Tunnel): Test[Either[LoaderError, Unit]] =
    State { log => ("SSH TUNNEL ESTABLISH" :: log, Right(())) }

  def getSchemas(vendor: String, name: String, model: Int): Test[Either[LoaderError, DSchemaList]] =
    SchemaList
      .parseStrings(List(s"iglu:$vendor/$name/jsonschema/$model-0-0"))
      .map { x => DSchemaList.fromSchemaList(x, TestInterpreter.fetch).value }
      .sequence[Test, Either[String, DSchemaList]]
      .map { e => e.flatten.leftMap { x => LoaderError.LoaderLocalError(x)} }


  private def trim(s: String): String =
    s.trim.replaceAll("\\s+", " ").replace("\n", " ")

}

