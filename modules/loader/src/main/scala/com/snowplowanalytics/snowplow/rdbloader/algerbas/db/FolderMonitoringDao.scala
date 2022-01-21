package com.snowplowanalytics.snowplow.rdbloader.algerbas.db

import com.snowplowanalytics.snowplow.rdbloader.common.S3

trait FolderMonitoringDao[C[_]] {
  def dropAlertingTempTable: C[Unit]
  def createAlertingTempTable: C[Unit]
  def foldersCopy(source: S3.Folder): C[Unit]
  def foldersMinusManifest: C[List[S3.Folder]]
}

object FolderMonitoringDao {
  def apply[C[_]](implicit ev: FolderMonitoringDao[C]): FolderMonitoringDao[C] = ev
}
