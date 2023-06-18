/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka

import cats.effect._
import com.snowplowanalytics.snowplow.rdbloader.azure.AzureBlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.parquet.ParquetOps
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.{Config, Run}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.generated.BuildInfo
import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s.ceTracking
import org.apache.hadoop.conf.Configuration

object Main extends IOApp {

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
