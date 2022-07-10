/*
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.db

import doobie.Fragment
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, S3}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.db.Columns.{ColumnsToCopy, ColumnsToSkip}
import com.snowplowanalytics.snowplow.rdbloader.discovery.ShreddedType
import com.snowplowanalytics.snowplow.rdbloader.loading.EventsTable


/**
 * ADT of all SQL statements the Loader can execute
 *
 * It does *not* represent Redshift (or other RDBMS) DDL AST
 * Instead it reflects all kinds of commands that *RDB Loader* can execute and tries to be
 * as specific with its needs as possible.
 *
 * By design a [[Statement]] is a ready-to-use independent SQL statement and isn't supposed
 * to be composable or boilerplate-free.
 *
 * It exists mostly to avoid passing around SQL-as-string because of potential SQL-injection
 * and SQL-as-fragment because it's useless in testing - all values are replaced with "?"
 */
sealed trait Statement

object Statement {

  sealed trait Loading extends Statement {
    def table: String
    def path: String
    def title = s"COPY $table FROM $path"
  }

  // Common
  case object Select1 extends Statement
  case object ReadyCheck extends Statement

  // Alerting
  case object CreateAlertingTempTable extends Statement
  case object DropAlertingTempTable extends Statement
  case object FoldersMinusManifest extends Statement
  case class FoldersCopy(source: S3.Folder) extends Statement

  // Loading
  case class EventsCopy(path: S3.Folder,
                        compression: Compression,
                        columnsToCopy: ColumnsToCopy,
                        columnsToSkip: ColumnsToSkip) extends Statement with Loading {
    def table: String = EventsTable.MainName
  }
  case class ShreddedCopy(shreddedType: ShreddedType, compression: Compression) extends Statement with Loading {
    def table: String = shreddedType.info.getName
    def path: String = shreddedType.getLoadPath
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
  case class ManifestGet(base: S3.Folder) extends Statement

  // Arbitrary-string DDL statements
  case class CreateTable(ddl: Fragment) extends Statement
  case class AlterTable(ddl: Fragment) extends Statement
  case class DdlFile(ddl: Fragment) extends Statement
}
