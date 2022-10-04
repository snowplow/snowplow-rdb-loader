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

import java.net.URI
import scala.concurrent.duration._
import cats.Parallel
import cats.implicits._
import cats.effect.concurrent.Ref
import cats.effect.{Blocker, Clock, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import com.snowplowanalytics.snowplow.rdbloader.aws.{S3, SQS}
import com.snowplowanalytics.snowplow.rdbloader.cloud.{JsonPathDiscovery, LoadAuthService}
import doobie.ConnectionIO
import org.http4s.client.blaze.BlazeClientBuilder
import io.sentry.{Sentry, SentryClient, SentryOptions}
import com.snowplowanalytics.snowplow.rdbloader.state.{Control, State}
import com.snowplowanalytics.snowplow.rdbloader.config.{CliConfig, Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Target
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics._
import com.snowplowanalytics.snowplow.rdbloader.utils.SSH
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.gcp.{GCS, Pubsub}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger


/** Container for most of interepreters to be used in Main
 * JDBC will be instantiated only when necessary, and as a `Reousrce`
 */
class Environment[F[_]](cache: Cache[F],
                        logging: Logging[F],
                        monitoring: Monitoring[F],
                        iglu: Iglu[F],
                        blobStorage: BlobStorage[F],
                        queueConsumer: Queue.Consumer[F],
                        loadAuthService: LoadAuthService[F],
                        jsonPathDiscovery: JsonPathDiscovery[F],
                        transaction: Transaction[F, ConnectionIO],
                        target: Target,
                        timeouts: Config.Timeouts,
                        control: Control[F]) {
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
  val controlF: Control[F] = control
}

object Environment {

  private implicit val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  case class CloudServices[F[_]](blobStorage: BlobStorage[F],
                                 queueConsumer: Queue.Consumer[F],
                                 loadAuthService: LoadAuthService[F],
                                 jsonPathDiscovery: JsonPathDiscovery[F])

  def initialize[F[_]: Clock: ConcurrentEffect: ContextShift: Timer: Parallel](cli: CliConfig, statementer: Target): Resource[F, Environment[F]] =
    for {
      blocker <- Blocker[F]
      implicit0(logger: Logger[F]) = Slf4jLogger.getLogger[F]
      httpClient <- BlazeClientBuilder[F](blocker.blockingContext).resource
      iglu <- Iglu.igluInterpreter(httpClient, cli.resolverConfig)
      implicit0(logging: Logging[F]) = Logging.loggingInterpreter[F](List(cli.config.storage.password.getUnencrypted, cli.config.storage.username))
      tracker <- Monitoring.initializeTracking[F](cli.config.monitoring, httpClient)
      sentry <- initSentry[F](cli.config.monitoring.sentry.map(_.dsn))
      statsdReporter = StatsDReporter.build[F](cli.config.monitoring.metrics.statsd, blocker)
      stdoutReporter = StdoutReporter.build[F](cli.config.monitoring.metrics.stdout)
      cacheMap <- Resource.eval(Ref.of[F, Map[String, Option[BlobStorage.Key]]](Map.empty))
      implicit0(cache: Cache[F]) = Cache.cacheInterpreter[F](cacheMap)
      state <- Resource.eval(State.mk[F])
      control = Control(state)
      cloudServices <- createCloudServices(cli.config, blocker, control)
      reporters = List(statsdReporter, stdoutReporter)
      periodicMetrics <- Resource.eval(Metrics.PeriodicMetrics.init[F](reporters, cli.config.monitoring.metrics.period))
      implicit0(monitoring: Monitoring[F]) = Monitoring.monitoringInterpreter[F](tracker, sentry, reporters, cli.config.monitoring.webhook, httpClient, periodicMetrics)
      _ <- SSH.resource(cli.config.storage.sshTunnel)
      transaction <- Transaction.interpreter[F](cli.config.storage, blocker)
    } yield new Environment[F](
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
      control
    )

  def initSentry[F[_]: Logging: Sync](dsn: Option[URI]): Resource[F, Option[SentryClient]] =
    dsn match {
      case Some(uri) =>
        val acquire = Sync[F].delay(Sentry.init(SentryOptions.defaults(uri.toString)))
        Resource
          .make(acquire)(client => Sync[F].delay(client.closeConnection()))
          .map(_.some)
          .evalTap { _ =>
            Logging[F].info(s"Sentry has been initialised at $uri")
          }

      case None =>
        Resource.pure[F, Option[SentryClient]](none[SentryClient])
    }

  def createCloudServices[F[_]: ConcurrentEffect: Timer: Logger: ContextShift: Cache](config: Config[StorageTarget], blocker: Blocker, control: Control[F]): Resource[F, CloudServices[F]] =
    config match {
      case c: Config.AWS[StorageTarget] =>
        for {
          s3Client <- Resource.eval(S3.getClient[F](c.region.name))
          implicit0(blobStorage: BlobStorage[F]) = S3.blobStorage[F](s3Client)
          postProcess = Queue.Consumer.postProcess[F]
          queueConsumer = SQS.consumer[F](c.messageQueue, c.timeouts.sqsVisibility, c.region.name, control.isBusy, Some(postProcess))
          loadAuthService <- LoadAuthService.aws[F](c.region.name, c.timeouts.loading)
          jsonPathDiscovery = JsonPathDiscovery.aws[F](c.region.name)
        } yield CloudServices(blobStorage, queueConsumer, loadAuthService, jsonPathDiscovery)
      case c: Config.GCP[StorageTarget] =>
        for {
          loadAuthService <- LoadAuthService.noop[F]
          jsonPathDiscovery = JsonPathDiscovery.noop[F]
          gcsClient = GCS.getClient(blocker)
          implicit0(blobStorage: BlobStorage[F]) = GCS.blobStorage(gcsClient)
          postProcess = Queue.Consumer.postProcess[F]
          queueConsumer <- Pubsub.consumer[F](
            blocker = blocker,
            projectId = c.projectId,
            subscription = c.subscriptionId,
            parallelPullCount = 1,
            bufferSize = 10,
            maxAckExtensionPeriod = 2.hours,
            customPubsubEndpoint = c.customPubsubEndpoint,
            postProcess = Some(postProcess)
          )
        } yield CloudServices(blobStorage, queueConsumer, loadAuthService, jsonPathDiscovery)
    }

}
