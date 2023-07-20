/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader

import scala.concurrent.duration.FiniteDuration
import io.circe._
import cats.{Applicative, Id, Show}
import cats.effect.Clock
import cats.implicits.toShow
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.iglu.client.ClientError
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryError
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data
import com.snowplowanalytics.snowplow.badrows.FailureDetails.LoaderIgluError

import java.util.concurrent.TimeUnit

package object common {

  implicit val catsClockIdInstance: Clock[Id] = new Clock[Id] {
    override def applicative: Applicative[Id] = Applicative[Id]

    override def monotonic: Id[FiniteDuration] = FiniteDuration(System.nanoTime(), TimeUnit.NANOSECONDS)

    override def realTime: Id[FiniteDuration] = FiniteDuration(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
  }

  /**
   * Syntax extension to transform `Either` with string as failure into circe-appropriate decoder
   * result
   */
  implicit class ParseErrorOps[A](val error: Either[String, A]) extends AnyVal {
    def asDecodeResult(hCursor: HCursor): Decoder.Result[A] = error match {
      case Right(success) => Right(success)
      case Left(message) => Left(DecodingFailure(message, hCursor.history))
    }
  }

  def isInputError(clientError: ClientError): Boolean =
    clientError match {
      case ClientError.ValidationError(_, _) =>
        false
      case ClientError.ResolutionError(map) =>
        map.values.toList.flatMap(_.errors.toList).exists {
          case RegistryError.RepoFailure(message) =>
            message.contains("exhausted input")
          case RegistryError.ClientFailure(message) =>
            message.contains("exhausted input")
          case RegistryError.NotFound =>
            false
        }
    }

  implicit def schemaCriterionConfigDecoder: Decoder[SchemaCriterion] =
    Decoder.decodeString.emap { s =>
      SchemaCriterion.parse(s).toRight(s"Cannot parse [$s] as Iglu SchemaCriterion, it must have iglu:vendor/name/format/1-*-* format")
    }

  implicit class ShredPropertyTransformer(val snowplowEntity: LoaderMessage.SnowplowEntity) extends AnyVal {
    def toSdkProperty: Data.ShredProperty = snowplowEntity match {
      case LoaderMessage.SnowplowEntity.Context => Data.Contexts(Data.CustomContexts)
      case LoaderMessage.SnowplowEntity.SelfDescribingEvent => Data.UnstructEvent
    }
  }

  implicit val loaderIgluErrorShow: Show[LoaderIgluError] = Show.show {
    case LoaderIgluError.IgluError(schemaKey, error) =>
      s"Iglu error for schema with the key: '${schemaKey.toSchemaUri}', error: ${error.show}"
    case LoaderIgluError.InvalidSchema(schemaKey, message) =>
      s"Invalid schema with key: '${schemaKey.toSchemaUri}, reason: $message"
    case LoaderIgluError.SchemaListNotFound(schemaCriterion, error) =>
      s"Schema list not found for criterion: '${schemaCriterion.asString}', error: ${error.show}"

    // for logging purposes we omit printing json values which may contain sensitive user-sent data
    case LoaderIgluError.WrongType(schemaKey, _, expected) =>
      s"Wrong type for field in schema with the key: '${schemaKey.toSchemaUri}', expected: $expected"
    case LoaderIgluError.NotAnArray(schemaKey, _, expected) =>
      s"Field is not an array in schema with the key: '${schemaKey.toSchemaUri}', expected: $expected"
    case LoaderIgluError.MissingInValue(schemaKey, key, _) =>
      s"Missing value for field: $key in schema with the key: '${schemaKey.toSchemaUri}'"
  }

  implicit val clientErrorShow: Show[ClientError] = Show.show {
    case error: ClientError.ResolutionError => error.getMessage
    case _: ClientError.ValidationError => "Validation error" // Should not really happen as loader only lookups schemas
  }
}
