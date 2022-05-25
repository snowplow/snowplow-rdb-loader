/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.processing

import cats.effect.concurrent.Deferred
import cats.effect.{Clock, ContextShift, IO, Sync, Timer}
import com.snowplowanalytics.aws.AWSQueue
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderCliConfig
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.Processing.Windowed
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sources.Parsed
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.{Processing, Resources}
import fs2.Stream
import org.typelevel.log4cats.slf4j.Slf4jLogger

object TestApplication {

  private def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def run(args: Seq[String],
          forCompletionMessage: Deferred[IO, String],
          windowedRecords: Stream[IO, Windowed[IO, Parsed]])
         (implicit CS: ContextShift[IO], T: Timer[IO], C: Clock[IO]): IO[Unit] =
    for {
      parsed <- ShredderCliConfig.Stream.loadConfigFrom[IO]("Streaming transformer", "Test app")(args).value
      res <- parsed match {
        case Right(cliConfig) =>
          Resources.mk[IO](
            cliConfig.igluConfig,
            cliConfig.config,
            queueFromDeferred(forCompletionMessage)
          )
          .use { resources =>
            logger[IO].info(s"Starting RDB Shredder with ${cliConfig.config} config") *>
              Processing.runWindowed[IO](windowedRecords, resources, cliConfig.config)
                .compile
                .drain
          }
        case Left(e) =>
          logger[IO].error(s"Configuration error: $e")
      }
    } yield res


  private def queueFromDeferred(deferred: Deferred[IO, String]): AWSQueue[IO] = new AWSQueue[IO] {
    override def sendMessage(groupId: Option[String], message: String): IO[Unit] = {
      deferred.complete(message)
    }
  }
}
