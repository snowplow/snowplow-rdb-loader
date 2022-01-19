package com.snowplowanalytics.snowplow.loader.snowflake.dsl

import cats.Monad
import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.FolderMonitoringDao
import com.snowplowanalytics.snowplow.loader.snowflake.db.SfDao
import com.snowplowanalytics.snowplow.loader.snowflake.config.SnowflakeTarget

class SnowflakeFolderMonitoringDao[C[_]: SfDao: Monad](target: SnowflakeTarget) extends FolderMonitoringDao[C] {
  override def dropAlertingTempTable: C[Unit] = Monad[C].pure(target) *> Monad[C].unit

  override def createAlertingTempTable: C[Unit] = Monad[C].unit

  override def foldersCopy(source: Folder): C[Unit] = Monad[C].unit

  override def foldersMinusManifest: C[List[Folder]] = Monad[C].pure(List.empty)
}
