/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
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
import com.snowplowanalytics.snowplow.rdbloader.discovery.DiscoveryFailure

object PureIglu {
  def interpreter: Iglu[Pure] = new Iglu[Pure] {
    def getSchemasWithSameModel(schemaKey: SchemaKey): Pure[Either[LoaderError, NonEmptyList[IgluSchema]]] =
      fetch(schemaKey).value
        .map(_.map(NonEmptyList.one))
        .map(_.leftMap(err => LoaderError.DiscoveryError(DiscoveryFailure.IgluError(err))))

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
