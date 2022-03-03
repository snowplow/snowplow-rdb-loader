/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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

import scala.concurrent.duration.{MILLISECONDS, NANOSECONDS, TimeUnit}

import io.circe._
import cats.Id
import cats.effect.Clock

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data

import com.snowplowanalytics.iglu.core.SchemaCriterion

import com.snowplowanalytics.iglu.client.ClientError
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryError

package object common {

  implicit val catsClockIdInstance: Clock[Id] = new Clock[Id] {
    override def realTime(unit: TimeUnit): Id[Long] =
      unit.convert(System.currentTimeMillis(), MILLISECONDS)

    override def monotonic(unit: TimeUnit): Id[Long] =
      unit.convert(System.nanoTime(), NANOSECONDS)
  }

  /**
    * Syntax extension to transform `Either` with string as failure
    * into circe-appropriate decoder result
    */
  implicit class ParseErrorOps[A](val error: Either[String, A]) extends AnyVal {
    def asDecodeResult(hCursor: HCursor): Decoder.Result[A] = error match {
      case Right(success) => Right(success)
      case Left(message) => Left(DecodingFailure(message, hCursor.history))
    }
  }

  def isInputError(clientError: ClientError): Boolean =
    clientError match {
      case ClientError.ValidationError(_) =>
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
    Decoder.decodeString.emap {
      s => SchemaCriterion.parse(s).toRight(s"Cannot parse [$s] as Iglu SchemaCriterion, it must have iglu:vendor/name/format/1-*-* format")
    }

  implicit class ShredPropertyTransformer(val snowplowEntity: LoaderMessage.SnowplowEntity) extends AnyVal {
    def toSdkProperty: Data.ShredProperty = snowplowEntity match {
      case LoaderMessage.SnowplowEntity.Contexts => Data.Contexts(Data.CustomContexts)
      case LoaderMessage.SnowplowEntity.SelfDescribingEvent => Data.UnstructEvent
    }
  }
}
