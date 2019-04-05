package com.snowplowanalytics.snowplow.storage.spark

import io.circe.{Encoder, Json}
import io.circe.syntax._
import cats.data.NonEmptyList
import com.snowplowanalytics.iglu.client.ClientError
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event


sealed trait BadRow {
  def toCompactJson: String
}

object BadRow {


  case class ShreddingError(original: String, errors: NonEmptyList[String]) extends BadRow {
    def toCompactJson: String =
      Json.obj(
        "original" := original.asJson,
        "errors" := errors.asJson
      ).noSpaces
  }

  case class ValidationError(original: Event, errors: NonEmptyList[SchemaError]) extends BadRow {
    def toCompactJson: String =
      Json.obj(
        "original" := original.asJson,
        "errors" := errors.asJson
      ).noSpaces
  }

  case class DeduplicationError(original: Event, error: String) extends BadRow {
    def toCompactJson: String = Json.obj(
      "original" := original.asJson,
      "error" := error.asJson
    ).noSpaces
  }

  case class SchemaError(schema: SchemaKey, error: ClientError)

  implicit val schemaErrorCirceJsonEncoder: Encoder[SchemaError] =
    Encoder.instance { case SchemaError(schema, error) =>
      error.asJson.deepMerge(Json.obj("schema" := schema.toSchemaUri.asJson))
    }
}
