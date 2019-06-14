/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.spark

import cats.data.NonEmptyList

import io.circe.{Encoder, Json}
import io.circe.syntax._
import io.circe.literal._

import com.snowplowanalytics.iglu.client.ClientError
import com.snowplowanalytics.iglu.core.{ SchemaKey, SchemaVer, SelfDescribingData }
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.storage.spark.ShredJob.Hierarchy
import com.snowplowanalytics.snowplow.rdbloader.common.Flattening.FlatteningError


sealed trait BadRow extends Product with Serializable {
  def toData: SelfDescribingData[Json]

  def toCompactJson: String =
    toData.normalize.noSpaces
}

object BadRow {

  val ParsingErrorSchema = SchemaKey("com.snowplowanalytics.snowplow.badrows", "loader_parsing_error", "jsonschema", SchemaVer.Full(1, 0, 0))
  val IgluErrorSchema = SchemaKey("com.snowplowanalytics.snowplow.badrows", "loader_iglu_error", "jsonschema", SchemaVer.Full(1, 0, 0))
  val RuntimeErrorSchema = SchemaKey("com.snowplowanalytics.snowplow.badrows", "loader_runtime_error", "jsonschema", SchemaVer.Full(1, 0, 0))

  val ShreddingErrorSchema = SchemaKey("com.snowplowanalytics.snowplow.badrows", "shredding_error", "jsonschema", SchemaVer.Full(1, 0, 0))

  final case class ShreddingError(payload: String, errors: NonEmptyList[String]) extends BadRow {
    def toData: SelfDescribingData[Json] =
      SelfDescribingData[Json](ParsingErrorSchema, json"""{"payload": $payload, "errors": $errors}""")
  }

  final case class ValidationError(original: Event, errors: NonEmptyList[SchemaError]) extends BadRow {
    def toData: SelfDescribingData[Json] =
      SelfDescribingData[Json](IgluErrorSchema, json"""{"event": $original, "errors": $errors}""")
  }

  final case class RuntimeError(original: Event, error: String) extends BadRow {
    def toData: SelfDescribingData[Json] =
      SelfDescribingData[Json](RuntimeErrorSchema, json"""{"event": $original, "error": $error}""")
  }

  final case class EntityShreddingError(hierarchy: Hierarchy, error: FlatteningError) extends BadRow { // TODO: won't compile - add schema
    def toData: SelfDescribingData[Json] =
      SelfDescribingData[Json](ShreddingErrorSchema, json"""{"event": $hierarchy, "error": $error}""")
  }

  final case class SchemaError(schema: SchemaKey, error: ClientError)

  implicit val schemaErrorCirceJsonEncoder: Encoder[SchemaError] =
    Encoder.instance { case SchemaError(schema, error) =>
      error.asJson.deepMerge(Json.obj("schema" := schema.toSchemaUri.asJson))
    }
}
