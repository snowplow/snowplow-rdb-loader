package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.Parallel
import cats.implicits._
import cats.effect._
import scala.concurrent.ExecutionContext
import com.snowplowanalytics.iglu.client.resolver.{InitListCache, InitSchemaCache}
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Config.{Monitoring, StreamInput}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sources.Checkpointer
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}

object Run {

  private def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  val InvalidConfig: ExitCode = ExitCode(2)

  def run[F[_]: ConcurrentEffect: ContextShift: Clock: InitSchemaCache: InitListCache: Timer: Parallel, C: Checkpointer[F, *]](
    args: List[String],
    buildName: String,
    buildVersion: String,
    buildDescription: String,
    ec: ExecutionContext,
    mkSource: (Blocker, StreamInput, Monitoring) => Resource[F, Queue.Consumer[F]],
    mkSink: (Blocker, Config.Output) => Resource[F, BlobStorage[F]],
    mkQueue: Config.QueueConfig => Resource[F, Queue.Producer[F]],
    checkpointer: Queue.Consumer.Message[F] => C
  ): F[ExitCode] =
    for {
      parsed <- CliConfig.loadConfigFrom[F](buildName, buildDescription)(args: Seq[String]).value
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
                     mkQueue,
                     checkpointer
                   )
                   .use { resources =>
                     import resources._
                     val processor = Processor(buildName, buildVersion)
                     logger[F].info(s"Starting RDB Shredder with ${cliConfig.config} config") *>
                       logger[F].info(s"RDB Shredder app id is  ${AppId.appId}") *>
                       Processing
                         .run[F, C](resources, cliConfig.config, processor)
                         .compile
                         .drain
                         .as(ExitCode.Success)
                   }
               case Left(e) =>
                 logger[F].error(s"Configuration error: $e").as(InvalidConfig)
             }
    } yield res
}
