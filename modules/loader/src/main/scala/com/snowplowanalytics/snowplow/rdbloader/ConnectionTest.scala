/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader

import java.time.Instant

import cats.{MonadThrow, Applicative}
import cats.effect.{Blocker, Clock, ContextShift, ExitCode, Sync, Resource}
import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.config.{Config, ConnectionTestConfig, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.common.{Common, S3}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.db.Statement
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.dsl.DAO.Purpose
import com.snowplowanalytics.snowplow.rdbloader.dsl.{AWS, DAO, Logging, Transaction}
import com.snowplowanalytics.snowplow.rdbloader.loading.Load

object ConnectionTest {

  val testEventId = "5710ea25-09f5-400a-88e6-50be56b95b01"

  /**
   * Write test transformed events to specified S3 folder.
   * Try to load this file to specified target.
   * After load operation is completed, query the target table
   * and check if the expected event is returned.
   * Return exit code according to the result.
   * After all the operations are completed, remove transformed events file
   * from S3 folder and delete the inserted events from the target table.
   */
  def run[F[_] : Sync : ContextShift : AWS : Logging : Transaction[*[_], C] : Clock,
    C[_] : MonadThrow : Logging : DAO](conf: Config[StorageTarget],
                                       connectionTestConf: ConnectionTestConfig,
                                       blocker: Blocker): F[ExitCode] = {
    for {
      now <- Clock[F].instantNow
      runId = createRunId(now)
      dd = createDataDiscovery(connectionTestConf.transformerOutput, runId)
      (q, d) <- transformedFileResource(conf.storage, connectionTestConf.transformerOutput, runId, blocker)
        .use { _ => executeTransactions(dd) }
      res <- handleResults[F](q, d)
    } yield res
  }

  /**
   * Create a resource which writes transformed events file to S3 folder
   * in the acquire step and remove that file in the release step.
   */
  def transformedFileResource[F[_] : Sync : AWS : ContextShift : Logging](storageTarget: StorageTarget,
                                                                          transformerOutput: S3.Folder,
                                                                          testRunId: String,
                                                                          blocker: Blocker): Resource[F, Unit] =
    Resource.make(writeTransformedFile(storageTarget, transformerOutput, testRunId, blocker)) {
      _ => removeTransformedFile(storageTarget, transformerOutput, testRunId)
    }

  /**
   * Execute database operations such as loading transformed events, querying the database
   * and deleting the test events in the end.
   */
  def executeTransactions[F[_] : Sync : Logging : Transaction[*[_], C],
    C[_] : DAO : MonadThrow : Logging](dataDiscovery: DataDiscovery): F[(List[String], Int)] = {
    val dummySetStageC: String => C[Unit] = _ => Transaction[F, C].arrowBack(Sync[F].unit)
    val load = Transaction[F, C].transact(
      Load.run[C](dummySetStageC, dataDiscovery)
    ) <* Logging[F].info(s"Load operation is completed")
    val query = for {
      r <- Transaction[F, C].transact(DAO[C].executeQueryList[String](Statement.TestQuery(testEventId)))
      _ <- Logging[F].info(s"Event id query result: $r")
    } yield r
    val delete = Transaction[F, C].transact(
      DAO[C].executeUpdate(Statement.DeleteEvent(testEventId), Purpose.NonLoading)
    ) <* Logging[F].info(s"Test event is deleted")
    (
      for {
        _ <- load
        queryResult <- query
        deleteResult <- delete
      } yield (queryResult, deleteResult)
      ).handleErrorWith { err =>
      Logging[F].error(s"Error during database operations: $err") *>
        delete.map(d => (List.empty[String], d))
    }
  }

  def handleResults[F[_] : Logging : Sync](queryResult: List[String], deletedRows: Int): F[ExitCode] =
    if (queryResult == List(testEventId) && deletedRows == 1)
      Logging[F].info("Test is completed successfully") *> Sync[F].pure(ExitCode.Success)
    else {
      val queryResultLog = if (queryResult != List(testEventId))
        Logging[F].error(s"Query returned unexpected result: $queryResult")
      else Sync[F].unit
      val deleteResultLog = if (deletedRows != 1)
        Logging[F].error(s"Count of deleted rows should be 1 but the actual count is $deletedRows")
      else Sync[F].unit
      Logging[F].error("Test failed") *>
        queryResultLog *>
        deleteResultLog *>
        Sync[F].pure(ExitCode.Error)
    }

  def writeTransformedFile[F[_] : Sync : AWS : ContextShift : Logging](storageTarget: StorageTarget,
                                                                       transformerOutput: S3.Folder,
                                                                       testRunId: String,
                                                                       blocker: Blocker): F[Unit] = {
    val transformedFileLocalPath = findInputFilePath(storageTarget)
    val fileInputStream = Sync[F].delay(getClass.getResourceAsStream(transformedFileLocalPath))
    val transformedFilePath = findOutputFilePath(storageTarget, transformerOutput, testRunId)
    fs2.io.readInputStream[F](fileInputStream, 4096, blocker)
      .through(AWS[F].sinkS3(transformedFilePath, true))
      .compile.drain *>
      Logging[F].info(s"Test transformed events file is written to $transformedFilePath")
  }

  def removeTransformedFile[F[_] : AWS : Applicative : Logging](storageTarget: StorageTarget, transformerOutput: S3.Folder, testRunId: String): F[Unit] =
    AWS[F].remove(findOutputFilePath(storageTarget, transformerOutput, testRunId)) *>
      Logging[F].info(s"Test transformed events file is removed")

  def createRunId(instant: Instant): String =
    s"run=${Common.FolderTimeFormatter.format(instant)}"

  def createDataDiscovery(transformerOutput: S3.Folder, runId: String): DataDiscovery =
    DataDiscovery(transformerOutput.append(runId), List.empty, Compression.Gzip)

  def findInputFilePath(storageTarget: StorageTarget): String = {
    val fileName = storageTarget match {
      case _: StorageTarget.Redshift => "shred-tsv"
      case _: StorageTarget.Snowflake => "widerow-json"
      case _: StorageTarget.Databricks => "widerow-parquet"
    }
    s"/connection-test/$fileName"
  }

  def findOutputFilePath(storageTarget: StorageTarget, transformerOutput: S3.Folder, testRunId: String): S3.Key = {
    val commonFolder = transformerOutput.append(testRunId).append("output=good")
    storageTarget match {
      case _: StorageTarget.Snowflake => commonFolder.withKey("test-event")
      case _: StorageTarget.Databricks => commonFolder.withKey("test-event")
      case _: StorageTarget.Redshift =>
        commonFolder
          .append("vendor=com.snowplowanalytics.snowplow")
          .append("name=atomic")
          .append("format=tsv")
          .append("model=1")
          .withKey("test-event")
    }
  }
}
