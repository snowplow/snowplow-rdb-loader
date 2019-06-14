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

import com.snowplowanalytics.iglu.client.ClientError
import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.storage.spark.ShredJob.Hierarchy
import com.snowplowanalytics.snowplow.rdbloader.common.Flattening.FlatteningError


sealed trait BadRow extends Product with Serializable {
  def toCompactJson: String
}

object BadRow {

  final case class ShreddingError(original: String, errors: NonEmptyList[String]) extends BadRow {
    def toCompactJson: String =
      Json.obj(
        "original" := original.asJson,
        "errors" := errors.asJson
      ).noSpaces
  }

  final case class ValidationError(original: Event, errors: NonEmptyList[SchemaError]) extends BadRow {
    def toCompactJson: String =
      Json.obj(
        "original" := original.asJson,
        "errors" := errors.asJson
      ).noSpaces
  }

  final case class DeduplicationError(original: Event, error: String) extends BadRow {
    def toCompactJson: String = Json.obj(
      "original" := original.asJson,
      "error" := error.asJson
    ).noSpaces
  }

  case class EntityShreddingError(original: Hierarchy, error: FlatteningError) extends BadRow {
    override def toCompactJson: String = Json.obj(
      "hierarchy" := original.asJson,
      "error" := error.asJson
    ).noSpaces
  }

  final case class SchemaError(schema: SchemaKey, error: ClientError)

  implicit val schemaErrorCirceJsonEncoder: Encoder[SchemaError] =
    Encoder.instance { case SchemaError(schema, error) =>
      error.asJson.deepMerge(Json.obj("schema" := schema.toSchemaUri.asJson))
    }
}
