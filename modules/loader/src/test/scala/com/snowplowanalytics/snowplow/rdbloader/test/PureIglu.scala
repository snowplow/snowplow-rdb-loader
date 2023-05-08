package com.snowplowanalytics.snowplow.rdbloader.test

import cats.data.{EitherT, NonEmptyList}
import cats.implicits._
import io.circe.literal._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.IgluSchema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.snowplow.badrows.FailureDetails
import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.SnowplowEntity
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow
import com.snowplowanalytics.snowplow.rdbloader.dsl.Iglu

object PureIglu {
  def interpreter: Iglu[Pure] = new Iglu[Pure] {
    def getSchemasWithSameModel(schemaKey: SchemaKey): Pure[Either[LoaderError, NonEmptyList[IgluSchema]]] =
      fetch(schemaKey).value
        .map(_.map(NonEmptyList.one))
        .map(_.leftMap(LoaderError.RuntimeError))

    override def fieldNamesFromTypes(types: List[WideRow.Type]): EitherT[Pure, FailureDetails.LoaderIgluError, List[String]] =
      types.traverse { `type` =>
        val context = `type`.snowplowEntity match {
          case SnowplowEntity.Context => "context"
          case SnowplowEntity.SelfDescribingEvent => "unstruct"
        }
        val k = `type`.schemaKey
        val name = s"${context}_${k.vendor}_${k.name}_${k.version.model}"
        EitherT.pure[Pure, FailureDetails.LoaderIgluError](name)
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
