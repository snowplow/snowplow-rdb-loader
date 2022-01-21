package com.snowplowanalytics.snowplow.rdbloader.test

import cats.data.EitherT
import cats.implicits._

import io.circe.literal._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaList, SchemaMap, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.IgluSchema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.iglu.schemaddl.migrations.{SchemaList => DSchemaList}

import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.dsl.Iglu

object PureIglu {
  implicit def interpreter: Iglu[Pure] = new Iglu[Pure] {
    def getSchemas(vendor: String, name: String, model: Int): Pure[Either[LoaderError, DSchemaList]] =
      SchemaList
        .parseStrings(List(s"iglu:$vendor/$name/jsonschema/$model-0-0"))
        .map { x =>
          DSchemaList.fromSchemaList(x, fetch).value
        }
        .sequence[Pure, Either[String, DSchemaList]]
        .map { e =>
          e.flatten.leftMap { x =>
            LoaderError.RuntimeError(x)
          }
        }
  }

  private def fetch(key: SchemaKey): EitherT[Pure, String, IgluSchema] = {
    val state = Pure { log =>
      val result = Schema.parse(json"""{}""").getOrElse(throw new RuntimeException("Not a valid JSON schema"))
      val schema = SelfDescribingSchema(SchemaMap(key), result)
      (log.log(s"Fetch ${key.toSchemaUri}"), schema)
    }
    EitherT.liftF(state)
  }
}
