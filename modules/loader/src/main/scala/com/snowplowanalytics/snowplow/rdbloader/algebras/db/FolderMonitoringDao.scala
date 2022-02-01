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
package com.snowplowanalytics.snowplow.rdbloader.algebras.db

import com.snowplowanalytics.snowplow.rdbloader.common.S3

/**
  * @tparam C - ConnectionIO
  *
  */
trait FolderMonitoringDao[C[_]] {
  def dropAlertingTempTable: C[Unit]
  def createAlertingTempTable: C[Unit]
  def foldersCopy(source: S3.Folder): C[Unit]
  def foldersMinusManifest: C[List[S3.Folder]]
}

object FolderMonitoringDao {
  def apply[C[_]](implicit ev: FolderMonitoringDao[C]): FolderMonitoringDao[C] = ev
}
