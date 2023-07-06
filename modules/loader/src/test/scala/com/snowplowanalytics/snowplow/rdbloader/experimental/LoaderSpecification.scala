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

import scala.concurrent.duration.DurationInt

import doobie.ConnectionIO

import cats.effect.{IO, Resource}
import cats.effect.std.Dispatcher
import cats.effect.unsafe.implicits.global

import fs2.concurrent.SignallingRef

import org.http4s.blaze.client.BlazeClientBuilder

import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics.Metrics
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, Monitoring, Transaction}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{Queue, SecretStore}
import com.snowplowanalytics.snowplow.rdbloader.common.integrationtestutils.ItUtils._
import com.snowplowanalytics.snowplow.rdbloader.common.integrationtestutils.InputBatch

import org.specs2.mutable.Specification

abstract class LoaderSpecification extends Specification with TestDAO.Provider with StorageTargetProvider with AzureTestResources {
  skipAllIf(anyEnvironmentVariableMissing())
  import LoaderSpecification._

  def run[A](
    inputBatches: List[InputBatch],
    countExpectations: CountExpectations,
    dbActions: TestDAO => IO[A]
  ): IO[(WindowsAccumulator[WindowOutput], A)] =
    createResources
      .use { resources =>
        for {
          _ <- resources.testDAO.cleanDb
          windowsAccumulator <- SignallingRef.of[IO, WindowsAccumulator[WindowOutput]](WindowsAccumulator(List.empty))
          consuming = consumeAllIncomingWindows[WindowOutput](
                        resources.queueConsumer,
                        countExpectations,
                        windowsAccumulator,
                        getWindowOutput = sc => IO.pure(WindowOutput(sc))
                      )
          producing = produceInput(inputBatches, resources.producer)
          _ <- consuming.concurrently(producing).compile.drain
          collectedWindows <- windowsAccumulator.get
          dbActionResult <- dbActions(resources.testDAO)
        } yield (collectedWindows, dbActionResult)
      }

  def createResources: Resource[IO, TestResources] =
    for {
      consumer <- createConsumer
      producer <- createProducer
      implicit0(secretStore: SecretStore[IO]) <- createSecretStore
      transaction <- createDbTransaction
      testDAO = createDAO(transaction)
    } yield TestResources(queueConsumer = consumer, producer = producer, testDAO = testDAO)

  def createDbTransaction(implicit secretStore: SecretStore[IO]): Resource[IO, Transaction[IO, ConnectionIO]] = {
    val storage: StorageTarget = createStorageTarget
    val timeouts: Config.Timeouts = Config.Timeouts(
      loading = 15.minutes,
      nonLoading = 15.minutes,
      sqsVisibility = 15.minutes,
      rollbackCommit = 15.minutes,
      connectionIsValid = 15.minutes
    )
    val readyCheck: Config.Retries = Config.Retries(
      strategy = Config.Strategy.Constant,
      attempts = Some(3),
      backoff = 30.seconds,
      cumulativeBound = None
    )
    for {
      implicit0(dispatcher: Dispatcher[IO]) <- Dispatcher.parallel[IO]
      httpClient <- BlazeClientBuilder[IO].withExecutionContext(global.compute).resource
      implicit0(logging: Logging[IO]) = Logging.loggingInterpreter[IO](List())
      periodicMetrics <- Resource.eval(Metrics.PeriodicMetrics.init[IO](List.empty, 1.minutes))
      implicit0(monitoring: Monitoring[IO]) =
        Monitoring.monitoringInterpreter[IO](None, None, List.empty, None, httpClient, periodicMetrics)
      transaction <- Transaction.interpreter[IO](storage, timeouts, readyCheck)
    } yield transaction
  }

  def anyEnvironmentVariableMissing(): Boolean =
    (getCloudResourcesEnvVars ::: getStorageTargetEnvVars).exists(varName => System.getenv(varName) == null)
}

object LoaderSpecification {

  final case class TestResources(
    queueConsumer: Queue.Consumer[IO],
    producer: Queue.Producer[IO],
    testDAO: TestDAO
  )

  final case class WindowOutput(
    shredding_complete: LoaderMessage.ShreddingComplete
  ) extends GetShreddingComplete
}
