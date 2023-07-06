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
package com.snowplowanalytics.snowplow.loader.snowflake.it

import scala.concurrent.duration.DurationInt

import cats.effect.IO

import doobie.ConnectionIO
import doobie.implicits._

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget
import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget.LoadAuthMethod
import com.snowplowanalytics.snowplow.rdbloader.dsl.Transaction
import com.snowplowanalytics.snowplow.rdbloader.experimental.{StorageTargetProvider, TestDAO}

trait SnowflakeTestResources extends TestDAO.Provider with StorageTargetProvider {

  val secretStoreParameterNameEnv = "TEST_LOADER_SECRET_STORE_PARAMETER"
  val snowflakeRegionEnv = "TEST_LOADER_SNOWFLAKE_REGION"
  val snowflakeUsernameEnv = "TEST_LOADER_SNOWFLAKE_USERNAME"
  val snowflakeRoleEnv = "TEST_LOADER_SNOWFLAKE_ROLE"
  val snowflakeAccountEnv = "TEST_LOADER_SNOWFLAKE_ACCOUNT"
  val snowflakeWarehouseEnv = "TEST_LOADER_SNOWFLAKE_WAREHOUSE"
  val snowflakeDatabaseEnv = "TEST_LOADER_SNOWFLAKE_DATABASE"
  val snowflakeSchemaEnv = "TEST_LOADER_SNOWFLAKE_SCHEMA"

  override def createDAO(transaction: Transaction[IO, ConnectionIO]): TestDAO = new TestDAO {
    override def cleanDb: IO[Unit] =
      for {
        _ <- transaction.run(sql"delete from atomic.events".update.run)
        _ <- transaction.run(sql"delete from atomic.manifest".update.run)
      } yield ()

    override def queryManifest: IO[List[LoaderMessage.ManifestItem]] =
      transaction.run(
        sql"""select base, types, shredding_started, shredding_completed,
              min_collector_tstamp, max_collector_tstamp,
              compression, processor_artifact, processor_version, count_good
              FROM atomic.manifest""".query[LoaderMessage.ManifestItem].stream.compile.toList
      )

    override def queryEventIds: IO[List[String]] =
      for {
        res <- transaction.run(sql"select event_id from atomic.events".query[String].stream.compile.toList)
      } yield res
  }

  override def createStorageTarget: StorageTarget =
    StorageTarget.Snowflake(
      snowflakeRegion = Some(System.getenv(snowflakeRegionEnv)),
      username = System.getenv(snowflakeUsernameEnv),
      role = Some(System.getenv(snowflakeRoleEnv)),
      password = StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig(System.getenv(secretStoreParameterNameEnv))),
      account = Some(System.getenv(snowflakeAccountEnv)),
      warehouse = System.getenv(snowflakeWarehouseEnv),
      database = System.getenv(snowflakeDatabaseEnv),
      schema = System.getenv(snowflakeSchemaEnv),
      transformedStage = None,
      appName = "loader-test",
      folderMonitoringStage = None,
      jdbcHost = None,
      loadAuthMethod = LoadAuthMethod.TempCreds.AzureTempCreds(15.minutes),
      readyCheck = StorageTarget.Snowflake.ResumeWarehouse
    )

  // For some reason, using variables above doesn't work here.
  // Therefore, we've used directly strings in here.
  override def getStorageTargetEnvVars: List[String] = List(
    "TEST_LOADER_SECRET_STORE_PARAMETER",
    "TEST_LOADER_SNOWFLAKE_REGION",
    "TEST_LOADER_SNOWFLAKE_USERNAME",
    "TEST_LOADER_SNOWFLAKE_ROLE",
    "TEST_LOADER_SNOWFLAKE_ACCOUNT",
    "TEST_LOADER_SNOWFLAKE_WAREHOUSE",
    "TEST_LOADER_SNOWFLAKE_DATABASE",
    "TEST_LOADER_SNOWFLAKE_SCHEMA"
  )
}
