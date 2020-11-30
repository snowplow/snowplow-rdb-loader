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
package com.snowplowanalytics.snowplow.rdbloader

import java.util.UUID

import scala.concurrent.duration.{TimeUnit, MILLISECONDS, NANOSECONDS}

import io.circe._

import cats.Id
import cats.effect.Clock

import com.snowplowanalytics.iglu.client.ClientError
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryError

import com.snowplowanalytics.snowplow.scalatracker.UUIDProvider

package object common {

  /** * Subpath to check `atomic-events` directory presence */
  val AtomicSubpathPattern = "(.*)/(run=[0-9]{4}-[0-1][0-9]-[0-3][0-9]-[0-2][0-9]-[0-6][0-9]-[0-6][0-9]/atomic-events)/(.*)".r
  //                                    year     month      day        hour       minute     second

  implicit val catsClockIdInstance: Clock[Id] = new Clock[Id] {
    override def realTime(unit: TimeUnit): Id[Long] =
      unit.convert(System.currentTimeMillis(), MILLISECONDS)

    override def monotonic(unit: TimeUnit): Id[Long] =
      unit.convert(System.nanoTime(), NANOSECONDS)
  }

  implicit val snowplowUuidIdInstance: UUIDProvider[Id] = new UUIDProvider[Id] {
    def generateUUID: Id[UUID] = UUID.randomUUID()
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
}
