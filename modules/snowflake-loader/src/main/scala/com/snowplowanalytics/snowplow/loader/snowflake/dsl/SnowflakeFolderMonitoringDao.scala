/*
 * Copyright (c) 2022-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.loader.snowflake.dsl

import cats.MonadThrow
import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.rdbloader.algebras.db.FolderMonitoringDao
import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.loader.snowflake.db.ast.{Column, SnowflakeDatatype}
import com.snowplowanalytics.snowplow.loader.snowflake.db.{SfDao, Statement, SnowflakeManifest, DbUtils}
import com.snowplowanalytics.snowplow.loader.snowflake.config.SnowflakeTarget

class SnowflakeFolderMonitoringDao[C[_]: SfDao: MonadThrow](target: SnowflakeTarget) extends FolderMonitoringDao[C] {
  import SnowflakeFolderMonitoringDao._

  override def dropAlertingTempTable: C[Unit] = {
    // Since this function is called first while checking the folders,
    // resume warehouse is only added in here.
    DbUtils.resumeWarehouse[C](target.warehouse) *>
      SfDao[C].executeUpdate(Statement.DropTable(target.schema, AlertingTempTableName)).as(())
  }

  override def createAlertingTempTable: C[Unit] = {
    SfDao[C].executeUpdate(
      Statement.CreateTable(
        target.schema,
        AlertingTempTableName,
        List(Column("run_id", SnowflakeDatatype.Varchar(512))),
        None,
        temporary = true
      )
    ).as(())
  }

  override def foldersCopy(source: Folder): C[Unit] = {
    val runId = source.split("/").last
    val loadPath = s"shredded/$runId"
    for {
      s <- folderMonitoringStage
      _ <- SfDao[C].executeUpdate(Statement.FoldersCopy(target.schema, AlertingTempTableName, s, loadPath))
    } yield ()
  }

  override def foldersMinusManifest: C[List[Folder]] =
    SfDao[C].executeQueryList(Statement.FoldersMinusManifest(target.schema, AlertingTempTableName, SnowflakeManifest.ManifestTable))

  def folderMonitoringStage: C[String] =
    target.folderMonitoringStage match {
      case None => MonadThrow[C].raiseError(LoaderError.StorageTargetError("Stage for folder monitoring isn't provided. Copy for folder monitoring couldn't be performed."))
      case Some(stage) => MonadThrow[C].pure(stage)
    }
}

object SnowflakeFolderMonitoringDao {
  val AlertingTempTableName = "rdb_folder_monitoring"
}
