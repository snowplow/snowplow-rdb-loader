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

import cats.effect.concurrent.Ref
import cats.effect.{Clock, ContextShift, IO, Sync, Timer}
import com.snowplowanalytics.aws.AWSQueue
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderCliConfig
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sources.ParsedF
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.{Processing, Resources}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sources.Checkpointer
import fs2.Stream
import org.typelevel.log4cats.slf4j.Slf4jLogger

object TestApplication {

  private def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def run(args: Seq[String],
          completionsRef: Ref[IO, Vector[String]],
          checkpointRef: Ref[IO, Int],
          sourceRecords: Stream[IO, ParsedF[IO, Unit]])
         (implicit CS: ContextShift[IO], T: Timer[IO], C: Clock[IO]): IO[Unit] =
    for {
      parsed <- ShredderCliConfig.Stream.loadConfigFrom[IO]("Streaming transformer", "Test app")(args).value
      implicit0(chk: Checkpointer[IO, Unit]) = checkpointer(checkpointRef)
      res <- parsed match {
        case Right(cliConfig) =>
          Resources.mk[IO](
            cliConfig.igluConfig,
            cliConfig.config,
            queueFromRef(completionsRef),
            concurrent.ExecutionContext.global
          )
          .use { resources =>
            logger[IO].info(s"Starting RDB Shredder with ${cliConfig.config} config") *>
              Processing.runFromSource[IO, Unit](sourceRecords, resources, cliConfig.config)
                .compile
                .drain
          }
        case Left(e) =>
          IO.raiseError(new RuntimeException(s"Configuration error: $e"))
      }
    } yield res


  private def queueFromRef(ref: Ref[IO, Vector[String]]): AWSQueue[IO] = new AWSQueue[IO] {
    override def sendMessage(groupId: Option[String], message: String): IO[Unit] =
      ref.update(_ :+ message)
  }

  def checkpointer(count: Ref[IO, Int]): Checkpointer[IO, Unit] = new Checkpointer[IO, Unit] {
    def checkpoint(c: Unit): IO[Unit] = count.update(_ + 1)
    def combine(older: Unit, newer: Unit): Unit = ()
    def empty: Unit = ()
  }
}
