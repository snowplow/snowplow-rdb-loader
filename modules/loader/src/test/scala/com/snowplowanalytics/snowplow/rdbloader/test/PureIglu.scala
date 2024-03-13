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
package com.snowplowanalytics.snowplow.rdbloader.test

import cats.data.EitherT
import cats.implicits._
import io.circe.literal._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaList, SchemaMap, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.IgluSchema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.iglu.schemaddl.migrations.{SchemaList => DSchemaList}
import com.snowplowanalytics.snowplow.badrows.FailureDetails
import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.SnowplowEntity
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow
import com.snowplowanalytics.snowplow.rdbloader.dsl.Iglu
import com.snowplowanalytics.snowplow.rdbloader.discovery.DiscoveryFailure

object PureIglu {
  def interpreter: Iglu[Pure] = new Iglu[Pure] {
    def getSchemas(
      vendor: String,
      name: String,
      model: Int
    ): Pure[Either[LoaderError, DSchemaList]] =
      SchemaList
        .parseStrings(List(s"iglu:$vendor/$name/jsonschema/$model-0-0"))
        .map(x => DSchemaList.fromSchemaList(x, fetch).value)
        .sequence[Pure, Either[String, DSchemaList]]
        .map(e => e.flatten.leftMap(x => LoaderError.DiscoveryError(DiscoveryFailure.IgluError(x))))

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
