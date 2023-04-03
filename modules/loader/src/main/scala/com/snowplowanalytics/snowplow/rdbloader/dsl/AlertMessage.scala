/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import cats.Show
import cats.implicits._
import java.sql.SQLException

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring.AlertPayload.Severity

sealed trait AlertMessage

object AlertMessage {

  implicit val throwableShow: Show[Throwable] = {
    def go(acc: List[String], next: Throwable): String = {
      val msg = next match {
        case t: SQLException => s"${t.getMessage} = SqlState: ${t.getSQLState}"
        case t => t.getMessage
      }
      Option(next.getCause) match {
        case Some(cause) => go(msg :: acc, cause)
        case None => (msg :: acc).reverse.mkString(": ")
      }
    }

    Show.show(go(Nil, _))
  }

  /** Restrict the length of an alert message to be compliant with alert iglu schema */
  private val MaxAlertPayloadLength = 4096

  case class FolderIsAlreadyLoaded(folder: BlobStorage.Folder) extends AlertMessage
  case class FolderIsUnloaded(folder: BlobStorage.Folder) extends AlertMessage
  case class ShreddingIncomplete(folder: BlobStorage.Folder) extends AlertMessage
  case object UnloadedFoldersDetected extends AlertMessage
  case class RetryableLoadFailure(folder: BlobStorage.Folder, cause: Throwable) extends AlertMessage
  case class TerminalLoadFailure(folder: BlobStorage.Folder, cause: Throwable) extends AlertMessage
  case class FailedInitialConnection(cause: Throwable) extends AlertMessage
  case class FailedToCreateEventsTable(cause: Throwable) extends AlertMessage
  case class FailedToCreateManifestTable(cause: Throwable) extends AlertMessage
  case class FailedHealthCheck(cause: Throwable) extends AlertMessage
  case class FailedFolderMonitoring(cause: Throwable, numFailures: Int) extends AlertMessage

  val getFolder: AlertMessage => Option[BlobStorage.Folder] = {
    // folder-specific messages
    case FolderIsAlreadyLoaded(folder) => Some(folder)
    case FolderIsUnloaded(folder) => Some(folder)
    case ShreddingIncomplete(folder) => Some(folder)
    case RetryableLoadFailure(folder, _) => Some(folder)
    case TerminalLoadFailure(folder, _) => Some(folder)
    // general messages
    case UnloadedFoldersDetected => None
    case FailedInitialConnection(_) => None
    case FailedToCreateEventsTable(_) => None
    case FailedToCreateManifestTable(_) => None
    case FailedHealthCheck(_) => None
    case FailedFolderMonitoring(_, _) => None
  }

  def getSeverity: AlertMessage => Severity = {
    // information: no action required to fix the problem
    case FolderIsAlreadyLoaded(_) => Severity.Info
    case RetryableLoadFailure(_, _) => Severity.Info
    // warnings: require external intervention to fix the problem
    case TerminalLoadFailure(_, _) => Severity.Warning
    case UnloadedFoldersDetected => Severity.Warning
    case FailedFolderMonitoring(_, _) => Severity.Warning
    case FailedHealthCheck(_) => Severity.Warning
    // These warnings might be downgraded to "info" in a future release because the
    // `UnloadedFoldersDetected` is already a call to action:
    case ShreddingIncomplete(_) => Severity.Warning
    case FolderIsUnloaded(_) => Severity.Warning
    // errors: these prevent the loader from running whatsoever. The loader will crash and exit after sending these alerts
    case FailedInitialConnection(_) => Severity.Error
    case FailedToCreateEventsTable(_) => Severity.Error
    case FailedToCreateManifestTable(_) => Severity.Error
  }

  def getString(am: AlertMessage): String = {
    val full = am match {
      case FolderIsAlreadyLoaded(_) => "Folder is already loaded"
      case FolderIsUnloaded(_) => "Unloaded batch"
      case ShreddingIncomplete(_) => "Incomplete shredding"
      case UnloadedFoldersDetected => "Folder monitoring detected unloaded folders"
      case RetryableLoadFailure(_, t) => show"Load failed and will be retried: $t"
      case TerminalLoadFailure(_, t) => show"Load failed and will not be retried: $t"
      case FailedInitialConnection(t) => show"Failed to get connection at startup: $t"
      case FailedToCreateEventsTable(t) => show"Failed to create events table: $t"
      case FailedToCreateManifestTable(t) => show"Failed to create manifest table: $t"
      case FailedHealthCheck(t) => show"DB failed health check: $t"
      case FailedFolderMonitoring(t, numFailures) => show"Folder monitoring failed $numFailures times in a row: $t"
    }
    full.take(MaxAlertPayloadLength)
  }
}
