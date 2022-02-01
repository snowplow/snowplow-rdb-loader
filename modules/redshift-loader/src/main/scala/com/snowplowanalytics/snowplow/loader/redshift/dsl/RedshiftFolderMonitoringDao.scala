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
package com.snowplowanalytics.snowplow.loader.redshift.dsl

import cats.Monad
import cats.syntax.all._
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.loader.redshift.config.RedshiftTarget
import com.snowplowanalytics.snowplow.loader.redshift.db.RsDao
import com.snowplowanalytics.snowplow.loader.redshift.db.Statement._
import com.snowplowanalytics.snowplow.rdbloader.algebras.db.FolderMonitoringDao

class RedshiftFolderMonitoringDao[C[_]: RsDao: Monad](target: RedshiftTarget) extends FolderMonitoringDao[C] {
  override def dropAlertingTempTable: C[Unit] = RsDao[C].executeUpdate(DropAlertingTempTable).as(())

  override def createAlertingTempTable: C[Unit] = RsDao[C].executeUpdate(CreateAlertingTempTable).as(())

  override def foldersCopy(source: Folder): C[Unit] =
    RsDao[C].executeUpdate(FoldersCopy(source, target.roleArn)).as(())

  override def foldersMinusManifest: C[List[Folder]] = RsDao[C].executeQueryList(FoldersMinusManifest(target.schema))
}
