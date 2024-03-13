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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka

import cats.effect._
import com.snowplowanalytics.snowplow.rdbloader.azure.AzureBlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.parquet.ParquetOps
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.{Config, Run}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.generated.BuildInfo
import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s.ceTracking
import org.apache.hadoop.conf.Configuration

import scala.concurrent.duration.DurationInt

object Main extends IOApp {

  override def runtimeConfig =
    super.runtimeConfig.copy(cpuStarvationCheckInterval = 10.seconds)

  def run(args: List[String]): IO[ExitCode] =
    Run.run[IO, KafkaCheckpointer[IO]](
      args,
      BuildInfo.name,
      BuildInfo.version,
      BuildInfo.description,
      runtime.compute,
      (config, _) => Queues.createInputQueue(config),
      c => createBlobStorage(c),
      c => Queues.createBadOutputQueue(c),
      Queues.createShreddingCompleteQueue,
      KafkaCheckpointer.checkpointer,
      parquetOps
    )

  private def createBlobStorage[F[_]: Async](output: Config.Output): Resource[F, BlobStorage[F]] =
    output match {
      case c: Config.Output.AzureBlobStorage =>
        AzureBlobStorage.createDefault[F](c.path)
      case _ =>
        Resource.eval(Async[F].raiseError(new IllegalArgumentException(s"Output is not Azure Blob Storage")))
    }

  private def parquetOps: ParquetOps = new ParquetOps {

    override def transformPath(p: String): String =
      AzureBlobStorage.PathParts.parse(p).toParquetPath

    override def hadoopConf: Configuration = {
      val hadoopConf = new Configuration()
      hadoopConf.set("fs.azure.account.auth.type", "Custom")
      hadoopConf.set("fs.azure.account.oauth.provider.type", "com.snowplowanalytics.snowplow.rdbloader.azure.AzureTokenProvider")
      hadoopConf
    }

  }
}
