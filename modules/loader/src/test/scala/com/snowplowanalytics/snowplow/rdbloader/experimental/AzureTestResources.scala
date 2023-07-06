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
package com.snowplowanalytics.snowplow.rdbloader.experimental

import java.util.UUID

import cats.effect.{IO, Resource}

import com.snowplowanalytics.snowplow.rdbloader.azure.{AzureKeyVault, KafkaConsumer, KafkaProducer}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{Queue, SecretStore}

trait AzureTestResources extends CloudResources {
  val eventHubsUrlEnv = "TEST_LOADER_EVENTHUBS_URL"
  val inputHubNameEnv = "TEST_LOADER_INPUT_HUB_NAME"
  val outputHubNameEnv = "TEST_LOADER_OUTPUT_HUB_NAME"
  val inputHubKeyEnv = "TEST_LOADER_INPUT_HUB_KEY"
  val outputHubKeyEnv = "TEST_LOADER_OUTPUT_HUB_KEY"
  val azureKeyVaultNameEnv = "TEST_LOADER_AZURE_KEY_VAULT_NAME"

  def createConsumer: Resource[IO, Queue.Consumer[IO]] =
    KafkaConsumer
      .consumer[IO](
        bootstrapServers = System.getenv(eventHubsUrlEnv),
        topicName = System.getenv(outputHubNameEnv),
        consumerConf = Map(
          "security.protocol" -> "SASL_SSL",
          "sasl.mechanism" -> "PLAIN",
          "sasl.jaas.config" ->
            s"""org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$$ConnectionString\" password=\"${System.getenv(
                outputHubKeyEnv
              )}\";""",
          "group.id" -> s"test-consumer-${UUID.randomUUID()}",
          "enable.auto.commit" -> "true",
          "auto.offset.reset" -> "latest"
        )
      )

  def createProducer: Resource[IO, Queue.Producer[IO]] =
    KafkaProducer
      .producer[IO](
        bootstrapServers = System.getenv(eventHubsUrlEnv),
        topicName = System.getenv(inputHubNameEnv),
        producerConf = Map(
          "security.protocol" -> "SASL_SSL",
          "sasl.mechanism" -> "PLAIN",
          "sasl.jaas.config" ->
            s"""org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$$ConnectionString\" password=\"${System.getenv(
                inputHubKeyEnv
              )}\";"""
        )
      )

  def createSecretStore: Resource[IO, SecretStore[IO]] =
    AzureKeyVault.create[IO](Some(System.getenv(azureKeyVaultNameEnv)))

  override def getCloudResourcesEnvVars: List[String] = List(
    eventHubsUrlEnv,
    inputHubNameEnv,
    outputHubNameEnv,
    inputHubKeyEnv,
    outputHubKeyEnv,
    azureKeyVaultNameEnv
  )
}
