/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import cats.implicits._

import cats.effect.{Sync, Clock, ConcurrentEffect}
import cats.effect.concurrent.Ref

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.config.CliConfig

/** Container for most of interepreters to be used in Main
 * JDBC will be instantiated only when necessary, and as a `Reousrce`
 */
class RealWorld[F[_]](cache: Cache[F], logging: Logging[F], iglu: Iglu[F], aws: AWS[F]) {
  implicit val cacheF: Cache[F] = cache
  implicit val loggingF: Logging[F] = logging
  implicit val igluF: Iglu[F] = iglu
  implicit val awsF: AWS[F] = aws
}

object RealWorld {
  def initialize[F[_] : ConcurrentEffect: Clock](config: CliConfig): F[RealWorld[F]] =
    for {
      cacheMap <- Ref.of[F, Map[String, Option[S3.Key]]](Map.empty)
      messages <- Ref.of[F, List[String]](List.empty[String])
      tracker <- Logging.initializeTracking[F](config.configYaml.monitoring)
      igluParsed <- Client.parseDefault[F](config.resolverConfig).value
      igluClient <- igluParsed match {
        case Right(client) => Sync[F].pure(client)
        case Left(error) => Sync[F].raiseError(error) // Should never happen because we already validated it
      }
      amazonS3 <- AWS.getClient[F](config.configYaml.aws)

      cache = Cache.cacheInterpreter[F](cacheMap)
      logging = Logging.controlInterpreter[F](config.target, messages, tracker)
      iglu = Iglu.igluInterpreter[F](igluClient)
      aws = AWS.s3Interpreter[F](amazonS3)
    } yield new RealWorld[F](cache, logging, iglu, aws)
}