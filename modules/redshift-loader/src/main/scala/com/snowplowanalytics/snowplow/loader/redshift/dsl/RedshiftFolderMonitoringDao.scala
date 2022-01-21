package com.snowplowanalytics.snowplow.loader.redshift.dsl

import cats.Monad
import cats.syntax.all._
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.loader.redshift.config.RedshiftTarget
import com.snowplowanalytics.snowplow.loader.redshift.db.RsDao
import com.snowplowanalytics.snowplow.loader.redshift.db.Statement._
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.FolderMonitoringDao

class RedshiftFolderMonitoringDao[C[_]: RsDao: Monad](target: RedshiftTarget) extends FolderMonitoringDao[C] {
  override def dropAlertingTempTable: C[Unit] = RsDao[C].executeUpdate(DropAlertingTempTable).as(())

  override def createAlertingTempTable: C[Unit] = RsDao[C].executeUpdate(CreateAlertingTempTable).as(())

  override def foldersCopy(source: Folder): C[Unit] =
    RsDao[C].executeUpdate(FoldersCopy(source, target.roleArn)).as(())

  override def foldersMinusManifest: C[List[Folder]] = RsDao[C].executeQueryList(FoldersMinusManifest(target.schema))
}
