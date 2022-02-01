package com.snowplowanalytics.snowplow.rdbloader.test.dao

import cats.implicits.toFunctorOps
import com.snowplowanalytics.snowplow.rdbloader.algebras.db.FolderMonitoringDao
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.test.Pure

object PureFolderMonitoringDao {
  implicit def interpreter: FolderMonitoringDao[Pure] = new FolderMonitoringDao[Pure] {
    def dropAlertingTempTable: Pure[Unit]          = Pure.sql("dropAlertingTempTable")
    def createAlertingTempTable: Pure[Unit]        = Pure.sql("createAlertingTempTable")
    def foldersCopy(source: S3.Folder): Pure[Unit] = Pure.sql(s"foldersCopy $source")
    def foldersMinusManifest: Pure[List[S3.Folder]] =
      Pure
        .sql("foldersMinusManifest")
        .as(
          List(S3.Folder.coerce("s3://bucket/shredded/run=2021-07-09-12-30-00/"))
        )
  }
}
