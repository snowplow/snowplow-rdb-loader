/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader.db

import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModel
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import doobie.Fragment
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.db.Columns.{ColumnsToCopy, ColumnsToSkip}
import com.snowplowanalytics.snowplow.rdbloader.cloud.authservice.LoadAuthService.LoadAuthMethod
import com.snowplowanalytics.snowplow.rdbloader.discovery.ShreddedType
import com.snowplowanalytics.snowplow.rdbloader.loading.EventsTable

/**
 * ADT of all SQL statements the Loader can execute
 *
 * It does *not* represent Redshift (or other RDBMS) DDL AST Instead it reflects all kinds of
 * commands that *RDB Loader* can execute and tries to be as specific with its needs as possible.
 *
 * By design a [[Statement]] is a ready-to-use independent SQL statement and isn't supposed to be
 * composable or boilerplate-free.
 *
 * It exists mostly to avoid passing around SQL-as-string because of potential SQL-injection and
 * SQL-as-fragment because it's useless in testing - all values are replaced with "?"
 */
sealed trait Statement

object Statement {

  sealed trait Loading extends Statement {
    def table: String
    def title: String
  }

  // Common
  case object Select1 extends Statement
  case object ReadyCheck extends Statement

  // Alerting
  case object CreateAlertingTempTable extends Statement
  case object DropAlertingTempTable extends Statement
  case object FoldersMinusManifest extends Statement
  case class FoldersCopy[T](
    source: BlobStorage.Folder,
    loadAuthMethod: LoadAuthMethod,
    initQueryResult: T
  ) extends Statement

  // Loading
  case class EventsCopy[T](
    path: BlobStorage.Folder,
    compression: Compression,
    columnsToCopy: ColumnsToCopy,
    columnsToSkip: ColumnsToSkip,
    typesInfo: TypesInfo,
    loadAuthMethod: LoadAuthMethod,
    initQueryResult: T
  ) extends Statement
      with Loading {
    def table: String = EventsTable.MainName
    def title = s"COPY $table FROM $path"
  }
  case class ShreddedCopy(
    shreddedType: ShreddedType,
    compression: Compression,
    loadAuthMethod: LoadAuthMethod,
    shredModel: ShredModel,
    tableName: String
  ) extends Statement
      with Loading {
    def table: String = shreddedType.info.getName
    def path: String = shreddedType.getLoadPath
    def title = s"COPY $table FROM $path"
  }
  case class CreateTempEventTable(table: String) extends Loading {
    def title: String = s"CREATE TEMP TABLE $table"
  }
  case class DropTempEventTable(table: String) extends Loading {
    def title: String = s"DROP TEMP TABLE $table"
  }
  case class EventsCopyToTempTable(
    path: BlobStorage.Folder,
    table: String,
    tempCreds: LoadAuthMethod.TempCreds,
    typesInfo: TypesInfo
  ) extends Loading {
    def title: String = s"COPY EVENTS FROM $path TO TEMP TABLE $table"
  }
  case class EventsCopyFromTempTable(table: String, columnsToCopy: ColumnsToCopy) extends Loading {
    def title: String = s"COPY EVENTS FROM TEMP TABLE $table TO ATOMIC.EVENTS TABLE"
  }
  case object CreateTransient extends Statement
  case object DropTransient extends Statement
  case object AppendTransient extends Statement

  // Migration
  case class TableExists(tableName: String) extends Statement
  case class GetVersion(tableName: String) extends Statement
  case class RenameTable(from: String, to: String) extends Statement

  case class GetColumns(tableName: String) extends Statement
  case class CommentOn(tableName: String, comment: String) extends Statement
  case object AddLoadTstampColumn extends Statement

  // Manifest
  case class ManifestAdd(message: LoaderMessage.ManifestItem) extends Statement
  case class ManifestGet(base: BlobStorage.Folder) extends Statement

  // Arbitrary-string DDL statements
  case class CreateTable(ddl: Fragment) extends Statement
  case class AlterTable(ddl: Fragment) extends Statement
  case class DdlFile(ddl: Fragment) extends Statement
  case object CreateDbSchema extends Statement

  // Optimize (housekeeping i.e. vacuum in redshift, optimize in databricks)
  case object VacuumManifest extends Statement
  case object VacuumEvents extends Statement

  case class StagePath(stage: String) extends Statement
}
