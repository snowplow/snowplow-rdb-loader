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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.Parallel
import cats.implicits._
import cats.data.EitherT
import cats.effect._

import scala.concurrent.ExecutionContext
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.parquet.ParquetOps
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Config.{Monitoring, StreamInput}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sources.Checkpointer
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.common.config.License
import com.snowplowanalytics.snowplow.scalatracker.Tracking

object Run {

  private def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  val InvalidConfig: ExitCode = ExitCode(2)

  def run[F[_]: Async: Parallel: Tracking, C: Checkpointer[F, *]](
    args: List[String],
    buildName: String,
    buildVersion: String,
    buildDescription: String,
    ec: ExecutionContext,
    mkSource: (StreamInput, Monitoring) => Resource[F, Queue.Consumer[F]],
    mkSink: Config.Output => Resource[F, BlobStorage[F]],
    mkBadQueue: Config.Output.Bad.Queue => Resource[F, Queue.ChunkProducer[F]],
    mkQueue: Config.QueueConfig => Resource[F, Queue.Producer[F]],
    checkpointer: Queue.Consumer.Message[F] => C,
    parquetOps: ParquetOps = ParquetOps.noop
  ): F[ExitCode] =
    for {
      parsed <- CliConfig
                  .loadConfigFrom[F](buildName, buildDescription)(args: Seq[String])
                  .flatTap(c => EitherT.fromEither[F](License.checkLicense(c.config.license)))
                  .value
      res <- parsed match {
               case Right(cliConfig) =>
                 Resources
                   .mk[F, C](
                     cliConfig.igluConfig,
                     cliConfig.config,
                     buildName,
                     buildVersion,
                     ec,
                     mkSource,
                     mkSink,
                     mkBadQueue,
                     mkQueue,
                     checkpointer,
                     parquetOps
                   )
                   .use { resources =>
                     val processor = Processor(buildName, buildVersion)
                     logger[F].info(s"Transformer app id is  ${AppId.appId}") *>
                       Processing
                         .run[F, C](resources, cliConfig.config, processor)
                         .compile
                         .drain
                         .onError { case error =>
                           logger[F].error("Transformer shutting down") *>
                             resources.sentry.fold(Sync[F].unit)(s => Sync[F].delay(s.sendException(error)))
                         }
                         .as(ExitCode.Success)
                   }
               case Left(e) =>
                 logger[F].error(s"Configuration error: $e").as(InvalidConfig)
             }
    } yield res

}
