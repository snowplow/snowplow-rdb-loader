package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.experimental
import cats.effect.IO
import cats.effect.kernel.Resource
import com.snowplowanalytics.snowplow.rdbloader.azure.{AzureBlobStorage, KafkaConsumer, KafkaProducer}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.experimental.AzureTransformerSpecification._

import java.net.URI
import java.util.UUID

// format: off
/**
 * Trait providing Azure specific dependencies for the generic `TransformerSpecification`. 
 * Before running tests with Azure support, it's necessary to:
 *
 *  1) Launch transformer application in the Azure cloud.
 *  2) Set up input (enriched data) and output (for `shredding_compelete.json` message to notify loader) EventHubs.
 *  3) Set up Azure Blob Storage account and container for transformed data. 
 *  4) Set following environment variables with values specific for your cloud setup:
 *
 *   - TEST_TRANSFORMER_BLOB_STORAGE_URL (format like 'https://${accountName}.blob.core.windows.net/${containerName})
 *   - TEST_TRANSFORMER_EVENTHUBS_URL (format like '...-namespace.servicebus.windows.net:9093")
 *   - TEST_TRANSFORMER_INPUT_HUB_NAME
 *   - TEST_TRANSFORMER_OUTPUT_HUB_NAME
 *   - TEST_TRANSFORMER_INPUT_HUB_KEY
 *   - TEST_TRANSFORMER_OUTPUT_HUB_KEY
 *   - AZURE_TENANT_ID - may be used by the azure-sdk library to communicate with the cloud if you wish to authenticate with Azure CLI. 
 *     It can be extracted e.g. with `az account show  | jq -r '.homeTenantId'` executed in command line.
 */
// format: on
trait AzureTransformerSpecification extends TransformerSpecification with AppDependencies.Provider {
  skipAllIf(anyEnvironmentVariableMissing())

  override def createDependencies(): Resource[IO, AppDependencies] =
    for {
      blobClient <- createBlobClient()
      consumer <- createConsumer()
      producer <- createProducer()
    } yield AppDependencies(blobClient, consumer, producer)

  private def createBlobClient(): Resource[IO, BlobStorage[IO]] =
    AzureBlobStorage
      .createDefault[IO](URI.create(System.getenv(blobStorageUrlEnv)))

  private def createConsumer(): Resource[IO, Queue.Consumer[IO]] =
    KafkaConsumer
      .consumer[IO](
        bootstrapServers = System.getenv(eventHubsUrlEnv),
        topicName = System.getenv(outputHubNameEnv),
        consumerConf = Map(
          "security.protocol" -> "SASL_SSL",
          "sasl.mechanism" -> "PLAIN",
          "sasl.jaas.config" -> s"""org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$$ConnectionString\" password=\"${System
              .getenv(outputHubKeyEnv)}\";""",
          "group.id" -> s"test-transformer-consumer-${UUID.randomUUID()}",
          "enable.auto.commit" -> "true",
          "auto.offset.reset" -> "latest"
        )
      )

  private def createProducer(): Resource[IO, Queue.Producer[IO]] =
    KafkaProducer
      .producer[IO](
        bootstrapServers = System.getenv(eventHubsUrlEnv),
        topicName = System.getenv(inputHubNameEnv),
        producerConf = Map(
          "security.protocol" -> "SASL_SSL",
          "sasl.mechanism" -> "PLAIN",
          "sasl.jaas.config" -> s"""org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$$ConnectionString\" password=\"${System
              .getenv(inputHubKeyEnv)}\";"""
        )
      )
}

object AzureTransformerSpecification {
  val blobStorageUrlEnv = "TEST_TRANSFORMER_BLOB_STORAGE_URL"
  val eventHubsUrlEnv = "TEST_TRANSFORMER_EVENTHUBS_URL"
  val inputHubNameEnv = "TEST_TRANSFORMER_INPUT_HUB_NAME"
  val outputHubNameEnv = "TEST_TRANSFORMER_OUTPUT_HUB_NAME"
  val inputHubKeyEnv = "TEST_TRANSFORMER_INPUT_HUB_KEY"
  val outputHubKeyEnv = "TEST_TRANSFORMER_OUTPUT_HUB_KEY"

  val requiredEnvironmentVariables = List(
    blobStorageUrlEnv,
    eventHubsUrlEnv,
    inputHubNameEnv,
    outputHubNameEnv,
    inputHubKeyEnv,
    outputHubKeyEnv
  )

  def anyEnvironmentVariableMissing(): Boolean =
    requiredEnvironmentVariables.exists(varName => System.getenv(varName) == null)

}
