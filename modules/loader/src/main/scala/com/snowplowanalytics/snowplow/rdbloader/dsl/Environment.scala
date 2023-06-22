/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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

import cats.Parallel
import cats.implicits._
import cats.effect.{Async, Resource}
import cats.effect.kernel.Ref
import cats.effect.std.{Dispatcher, Random}
import cats.effect.unsafe.implicits.global
import doobie.ConnectionIO
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.snowplowanalytics.snowplow.rdbloader.common.Sentry
import com.snowplowanalytics.snowplow.rdbloader.common.telemetry.Telemetry
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue, SecretStore}
import com.snowplowanalytics.snowplow.rdbloader.aws.{EC2ParameterStore, S3, SQS}
import com.snowplowanalytics.snowplow.rdbloader.azure.{AzureBlobStorage, AzureKeyVault, KafkaConsumer}
import com.snowplowanalytics.snowplow.rdbloader.gcp.{GCS, Pubsub, SecretManager}
import com.snowplowanalytics.snowplow.rdbloader.cloud.JsonPathDiscovery
import com.snowplowanalytics.snowplow.rdbloader.cloud.authservice.{AWSAuthService, AzureAuthService, LoadAuthService}
import com.snowplowanalytics.snowplow.rdbloader.state.{Control, State}
import com.snowplowanalytics.snowplow.rdbloader.config.{CliConfig, Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.config.Config.Cloud
import com.snowplowanalytics.snowplow.rdbloader.db.Target
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics._
import com.snowplowanalytics.snowplow.scalatracker.Tracking
import org.http4s.blaze.client.BlazeClientBuilder

/**
 * Container for most of interepreters to be used in Main JDBC will be instantiated only when
 * necessary, and as a `Reousrce`
 */
class Environment[F[_], I](
  cache: Cache[F],
  logging: Logging[F],
  monitoring: Monitoring[F],
  iglu: Iglu[F],
  blobStorage: BlobStorage[F],
  queueConsumer: Queue.Consumer[F],
  loadAuthService: LoadAuthService[F],
  jsonPathDiscovery: JsonPathDiscovery[F],
  transaction: Transaction[F, ConnectionIO],
  target: Target[I],
  timeouts: Config.Timeouts,
  control: Control[F],
  telemetry: Telemetry[F]
) {
  implicit val cacheF: Cache[F] = cache
  implicit val loggingF: Logging[F] = logging
  implicit val monitoringF: Monitoring[F] = monitoring
  implicit val igluF: Iglu[F] = iglu
  implicit val blobStorageF: BlobStorage[F] = blobStorage
  implicit val queueConsumerF: Queue.Consumer[F] = queueConsumer
  implicit val loadAuthServiceF: LoadAuthService[F] = loadAuthService
  implicit val jsonPathDiscoveryF: JsonPathDiscovery[F] = jsonPathDiscovery
  implicit val transactionF: Transaction[F, ConnectionIO] = transaction

  implicit val daoC: DAO[ConnectionIO] = DAO.connectionIO(target, timeouts)
  implicit val loggingC: Logging[ConnectionIO] = logging.mapK(transaction.arrowBack)
  implicit val loadAuthServiceC: LoadAuthService[ConnectionIO] = loadAuthService.mapK(transaction.arrowBack)
  val controlF: Control[F] = control
  val telemetryF: Telemetry[F] = telemetry
  val dbTarget: Target[I] = target
}

object Environment {

  val appId = java.util.UUID.randomUUID.toString

  case class CloudServices[F[_]](
    blobStorage: BlobStorage[F],
    queueConsumer: Queue.Consumer[F],
    loadAuthService: LoadAuthService[F],
    jsonPathDiscovery: JsonPathDiscovery[F],
    secretStore: SecretStore[F]
  )

  def initialize[F[_]: Async: Parallel: Tracking, I](
    cli: CliConfig,
    statementer: Target[I],
    appName: String,
    appVersion: String
  ): Resource[F, Environment[F, I]] =
    for {
      httpClient <- BlazeClientBuilder[F].withExecutionContext(global.compute).resource
      implicit0(logger: Logger[F]) = Slf4jLogger.getLogger[F]
      iglu <- Iglu.igluInterpreter(httpClient, cli.resolverConfig)
      implicit0(logging: Logging[F]) =
        Logging.loggingInterpreter[F](List(cli.config.storage.password.getUnencrypted, cli.config.storage.username))
      implicit0(random: Random[F]) <- Resource.eval(Random.scalaUtilRandom[F])
      tracker <- Monitoring.initializeTracking[F](cli.config.monitoring, httpClient)
      sentry <- Sentry.init[F](cli.config.monitoring.sentry.map(_.dsn))
      statsdReporter = StatsDReporter.build[F](cli.config.monitoring.metrics.statsd)
      stdoutReporter = StdoutReporter.build[F](cli.config.monitoring.metrics.stdout)
      cacheMap <- Resource.eval(Ref.of[F, Map[String, Option[BlobStorage.Key]]](Map.empty))
      implicit0(cache: Cache[F]) = Cache.cacheInterpreter[F](cacheMap)
      state <- Resource.eval(State.mk[F])
      control = Control[F](state)
      cloudServices <- createCloudServices[F](cli.config, control)
      reporters = List(statsdReporter, stdoutReporter)
      periodicMetrics <- Resource.eval(Metrics.PeriodicMetrics.init[F](reporters, cli.config.monitoring.metrics.period))
      implicit0(monitoring: Monitoring[F]) =
        Monitoring.monitoringInterpreter[F](tracker, sentry, reporters, cli.config.monitoring.webhook, httpClient, periodicMetrics)
      implicit0(secretStore: SecretStore[F]) = cloudServices.secretStore
      implicit0(dispatcher: Dispatcher[F]) <- Dispatcher.parallel[F]
      transaction <- Transaction.interpreter[F](cli.config.storage, cli.config.timeouts, cli.config.readyCheck)
      transaction <- Resource.pure(RetryingTransaction.wrap(cli.config.retries, transaction))
      telemetry <- Telemetry.build[F](
                     cli.config.telemetry,
                     appName,
                     appVersion,
                     httpClient,
                     appId,
                     getRegionForTelemetry(cli.config),
                     getCloudForTelemetry(cli.config)
                   )
    } yield new Environment[F, I](
      cache,
      logging,
      monitoring,
      iglu,
      cloudServices.blobStorage,
      cloudServices.queueConsumer,
      cloudServices.loadAuthService,
      cloudServices.jsonPathDiscovery,
      transaction,
      statementer,
      cli.config.timeouts,
      control,
      telemetry
    )

  def createCloudServices[F[_]: Async: Logger: Cache](
    config: Config[StorageTarget],
    control: Control[F]
  ): Resource[F, CloudServices[F]] =
    config.cloud match {
      case c: Cloud.AWS =>
        for {
          implicit0(blobStorage: BlobStorage[F]) <- S3.blobStorage[F](c.region.name)
          postProcess = Queue.Consumer.postProcess[F]
          queueConsumer <-
            SQS.consumer[F](
              c.messageQueue.queueName,
              config.timeouts.sqsVisibility,
              c.messageQueue.region.getOrElse(c.region).name,
              control.isBusy,
              Some(postProcess)
            )
          loadAuthService <-
            AWSAuthService
              .create[F](c.region.name, config.storage.eventsLoadAuthMethod, config.storage.foldersLoadAuthMethod)
          jsonPathDiscovery = JsonPathDiscovery.aws[F](c.region.name)
          secretStore <- EC2ParameterStore.secretStore[F]
        } yield CloudServices(blobStorage, queueConsumer, loadAuthService, jsonPathDiscovery, secretStore)
      case c: Cloud.GCP =>
        for {
          loadAuthService <- LoadAuthService.noop[F]
          jsonPathDiscovery = JsonPathDiscovery.noop[F]
          implicit0(blobStorage: BlobStorage[F]) <- GCS.blobStorage
          postProcess = Queue.Consumer.postProcess[F]
          queueConsumer <- Pubsub.consumer[F](
                             projectId = c.messageQueue.projectId,
                             subscription = c.messageQueue.subscriptionId,
                             parallelPullCount = c.messageQueue.parallelPullCount,
                             bufferSize = c.messageQueue.bufferSize,
                             maxAckExtensionPeriod = config.timeouts.loading,
                             customPubsubEndpoint = c.messageQueue.customPubsubEndpoint,
                             postProcess = Some(postProcess)
                           )
          secretStore <- SecretManager.secretManager[F]
        } yield CloudServices(blobStorage, queueConsumer, loadAuthService, jsonPathDiscovery, secretStore)
      case c: Cloud.Azure =>
        for {
          loadAuthService <-
            AzureAuthService
              .create[F](c.blobStorageEndpoint.toString, config.storage.eventsLoadAuthMethod, config.storage.foldersLoadAuthMethod)
          jsonPathDiscovery = JsonPathDiscovery.noop[F]
          implicit0(blobStorage: BlobStorage[F]) <- AzureBlobStorage.createDefault[F](c.blobStorageEndpoint)
          postProcess = Queue.Consumer.postProcess[F]
          queueConsumer <- KafkaConsumer.consumer[F](
                             bootstrapServers = c.messageQueue.bootstrapServers,
                             topicName = c.messageQueue.topicName,
                             consumerConf = c.messageQueue.consumerConf,
                             postProcess = Some(postProcess)
                           )
          secretStore <- AzureKeyVault.create(c.azureVaultName)
        } yield CloudServices(blobStorage, queueConsumer, loadAuthService, jsonPathDiscovery, secretStore)
    }

  def getCloudForTelemetry(config: Config[_]): Option[Telemetry.Cloud] =
    config.cloud match {
      case _: Cloud.AWS => Telemetry.Cloud.Aws.some
      case _: Cloud.GCP => Telemetry.Cloud.Gcp.some
      case _: Cloud.Azure => Telemetry.Cloud.Azure.some
    }

  def getRegionForTelemetry(config: Config[_]): Option[String] =
    config.cloud match {
      case c: Cloud.AWS => c.region.name.some
      case _ => None
    }

}
