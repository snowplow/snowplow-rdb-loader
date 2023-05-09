/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.common.config

import cats.data.EitherT
import cats.effect.Sync
import cats.syntax.either._
import cats.syntax.show._
import com.snowplowanalytics.snowplow.rdbloader.common.config.args._
import com.typesafe.config.{Config => TypesafeConfig, ConfigFactory}
import io.circe._
import io.circe.config.syntax.CirceConfigOps

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

object ConfigUtils {

  def parseAppConfigF[F[_]: Sync, A: Decoder](in: HoconOrPath): EitherT[F, String, A] =
    EitherT(Sync[F].delay(parseAppConfig(in)))

  def parseJsonF[F[_]: Sync](in: HoconOrPath): EitherT[F, String, Json] =
    EitherT(Sync[F].delay(parseJson(in)))

  def parseAppConfig[A: Decoder](in: HoconOrPath): Either[String, A] =
    parseHoconOrPath(in, appConfigFallbacks)

  def parseJson(in: HoconOrPath): Either[String, Json] =
    parseHoconOrPath[Json](in, identity)

  def hoconFromString(str: String): Either[String, DecodedHocon] =
    Either
      .catchNonFatal(DecodedHocon(ConfigFactory.parseString(str)))
      .leftMap(_.getMessage)

  private def parseHoconOrPath[A: Decoder](
    config: HoconOrPath,
    fallbacks: TypesafeConfig => TypesafeConfig
  ): Either[String, A] =
    config match {
      case Left(hocon) =>
        resolve[A](hocon, fallbacks)
      case Right(path) =>
        readTextFrom(path)
          .flatMap(hoconFromString)
          .flatMap(hocon => resolve[A](hocon, fallbacks))
    }

  private def resolve[A: Decoder](hocon: DecodedHocon, fallbacks: TypesafeConfig => TypesafeConfig): Either[String, A] = {
    val either = for {
      resolved <- Either.catchNonFatal(hocon.value.resolve()).leftMap(_.getMessage)
      merged <- Either.catchNonFatal(fallbacks(resolved)).leftMap(_.getMessage)
      parsed <- merged.as[A].leftMap(_.show)
    } yield parsed
    either.leftMap(e => s"Cannot resolve config: $e")
  }

  private def readTextFrom(path: Path): Either[String, String] =
    Either
      .catchNonFatal(Files.readAllLines(path).asScala.mkString("\n"))
      .leftMap(e => s"Error reading ${path.toAbsolutePath} file from filesystem: ${e.getMessage}")

  private def appConfigFallbacks(config: TypesafeConfig): TypesafeConfig =
    namespaced(ConfigFactory.load(namespaced(config.withFallback(namespaced(ConfigFactory.load())))))

  private def namespaced(config: TypesafeConfig): TypesafeConfig = {
    val namespace = "snowplow"
    if (config.hasPath(namespace))
      config.getConfig(namespace).withFallback(config.withoutPath(namespace))
    else
      config
  }
}
