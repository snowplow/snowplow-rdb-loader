/*
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.core.db

import doobie.Fragment

/**
  * By design a [[Statement]] is a ready-to-use independent SQL statement and isn't supposed
  * to be composable or boilerplate-free.
  *
  * It exists mostly to avoid passing around SQL-as-string because of potential SQL-injection
  * and SQL-as-fragment because it's useless in testing - all values are replaced with "?"
  */
trait Statement {

  /** Transform to doobie `Fragment`, closer to the end-of-the-world */
  def toFragment: Fragment
}

object Statement {
  // Transactions
  trait BeginStatement extends Statement
  trait CommitStatement extends Statement
  trait AbortStatement extends Statement

  // Folder monitoring
  trait CreateAlertingTempTableStatement extends Statement
  trait DropAlertingTempTableStatement extends Statement
  trait FoldersCopyStatement extends Statement
  trait FoldersMinusManifestStatement extends Statement
}
