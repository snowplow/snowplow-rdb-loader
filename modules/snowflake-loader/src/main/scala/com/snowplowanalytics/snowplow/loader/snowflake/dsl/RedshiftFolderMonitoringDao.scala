package com.snowplowanalytics.snowplow.loader.snowflake.dsl

import cats.Monad
import cats.syntax.all._
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.loader.snowflake.config.RedshiftTarget
import com.snowplowanalytics.snowplow.loader.snowflake.db.RsDao
import com.snowplowanalytics.snowplow.loader.snowflake.db.Statement._
import com.snowplowanalytics.snowplow.rdbloader.algebras.db.FolderMonitoringDao

class RedshiftFolderMonitoringDao[C[_]: SfDao: Monad](target: RedshiftTarget) extends FolderMonitoringDao[C] {
  override def dropAlertingTempTable: C[Unit] = ???

  override def createAlertingTempTable: C[Unit] = ???

  override def foldersCopy(source: Folder): C[Unit] = ???

  override def foldersMinusManifest: C[List[Folder]] = ???
}
