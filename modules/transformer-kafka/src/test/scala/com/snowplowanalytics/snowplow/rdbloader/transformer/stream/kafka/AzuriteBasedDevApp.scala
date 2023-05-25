/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka

import cats.effect._
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.common.StorageSharedKeyCredential
import com.snowplowanalytics.snowplow.rdbloader.azure.AzureBlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.{Config, Run}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.generated.BuildInfo
import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s.ceTracking

import java.util.Base64

/**
 * Test transformer application that can be run locally with local Azure-like resources.
 *
 * To run following resources are required:
 *
 *   - Kafka cluster (localhost:9092) with two topics: 'enriched' (input) and 'shreddingComplete'
 *     (for shredding complete message to notify loader)
 *   - Azurite Blob Storage (http://127.0.0.1:10000/devstoreaccount1) with `transformed` blob
 *     container created
 *
 * In the future it could be converted to automatic integration tests using testcontainers.
 */
object AzuriteBasedDevApp extends IOApp {

  val appConfig =
    """
      |{
      |  "input": {
      |    "topicName": "enriched"
      |    "bootstrapServers": "localhost:9092"
      |  }
      |  "output": {
      |     "path": "http://127.0.0.1:10000/devstoreaccount1/transformed"
      |  }
      |  "windowing": "1 minute"
      |  
      |  "queue": {
      |    "topicName": "shreddingComplete"
      |    "bootstrapServers": "localhost:9092"
      |  }
      |}
      |""".stripMargin

  val resolverConfig =
    """
      |{
      |  "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
      |  "data": {
      |    "cacheSize": 500,
      |    "cacheTtl": 30,
      |    "repositories": [
      |      {
      |        "name": "Iglu Central",
      |        "priority": 0,
      |        "vendorPrefixes": [ ],
      |        "connection": {
      |          "http": {
      |            "uri": "http://iglucentral.com"
      |          }
      |        }
      |      }
      |    ]
      |  }
      |}
      |""".stripMargin

  val credentials = new StorageSharedKeyCredential(
    "devstoreaccount1",
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
  )

  def run(args: List[String]): IO[ExitCode] = {
    val fixedArgs = List("--config", encode(appConfig), "--iglu-config", encode(resolverConfig))
    Run.run[IO, KafkaCheckpointer[IO]](
      fixedArgs,
      BuildInfo.name,
      BuildInfo.version,
      BuildInfo.description,
      runtime.compute,
      (config, _) => Queues.createInputQueue(config),
      c => createBlobStorageWithAzuriteKeys(c),
      c => Queues.createBadOutputQueue(c),
      Queues.createShreddingCompleteQueue,
      KafkaCheckpointer.checkpointer
    )
  }

  private def createBlobStorageWithAzuriteKeys[F[_]: Async](output: Config.Output): Resource[F, BlobStorage[F]] =
    output match {
      case c: Config.Output.AzureBlobStorage =>
        val clientBuilder = new BlobServiceClientBuilder().credential(credentials)
        AzureBlobStorage.create(c.path, clientBuilder)
      case _ =>
        Resource.eval(Async[F].raiseError(new IllegalArgumentException(s"Output is not Azure Blob Storage")))
    }

  private def encode(value: String) =
    new String(Base64.getUrlEncoder.encode(value.getBytes("UTF-8")))

}
