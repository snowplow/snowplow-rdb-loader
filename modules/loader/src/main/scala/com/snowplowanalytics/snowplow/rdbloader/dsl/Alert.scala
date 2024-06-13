/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader.dsl

import cats.Show
import cats.implicits._
import java.sql.SQLException

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring.AlertPayload.Severity

sealed trait Alert

object Alert {

  /** Restrict the length of an alert message to be compliant with alert iglu schema */
  private val MaxAlertPayloadLength = 4096

  case class FolderIsAlreadyLoaded(folder: BlobStorage.Folder) extends Alert
  case class FolderIsUnloaded(folder: BlobStorage.Folder) extends Alert
  case class ShreddingIncomplete(folder: BlobStorage.Folder) extends Alert
  case object UnloadedFoldersDetected extends Alert
  case class UnskippableLoadFailure(folder: BlobStorage.Folder, cause: Throwable) extends Alert
  case class RetryableLoadFailure(folder: BlobStorage.Folder, cause: Throwable) extends Alert
  case class TerminalLoadFailure(folder: BlobStorage.Folder, cause: Throwable) extends Alert
  case class FailedInitialConnection(cause: Throwable) extends Alert
  case class FailedToCreateEventsTable(cause: Throwable) extends Alert
  case class FailedToCreateManifestTable(cause: Throwable) extends Alert
  case class FailedToCreateDatabaseSchema(cause: Throwable) extends Alert
  case class FailedHealthCheck(cause: Throwable) extends Alert
  case class FailedFolderMonitoring(cause: Throwable, numFailures: Int) extends Alert

  def getFolder(alert: Alert): Option[BlobStorage.Folder] = alert match {
    // folder-specific messages
    case FolderIsAlreadyLoaded(folder)     => Some(folder)
    case FolderIsUnloaded(folder)          => Some(folder)
    case ShreddingIncomplete(folder)       => Some(folder)
    case UnskippableLoadFailure(folder, _) => Some(folder)
    case RetryableLoadFailure(folder, _)   => Some(folder)
    case TerminalLoadFailure(folder, _)    => Some(folder)
    // general messages
    case UnloadedFoldersDetected         => None
    case FailedInitialConnection(_)      => None
    case FailedToCreateEventsTable(_)    => None
    case FailedToCreateManifestTable(_)  => None
    case FailedToCreateDatabaseSchema(_) => None
    case FailedHealthCheck(_)            => None
    case FailedFolderMonitoring(_, _)    => None
  }

  def getSeverity(alert: Alert): Severity = alert match {
    // information: no action required to fix the problem
    case FolderIsAlreadyLoaded(_)   => Severity.Info
    case RetryableLoadFailure(_, _) => Severity.Info
    // warnings: require external intervention to fix the problem
    case UnskippableLoadFailure(_, _) => Severity.Warning
    case TerminalLoadFailure(_, _)    => Severity.Warning
    case UnloadedFoldersDetected      => Severity.Warning
    case FailedFolderMonitoring(_, _) => Severity.Warning
    case FailedHealthCheck(_)         => Severity.Warning
    // These warnings might be downgraded to "info" in a future release because the
    // `UnloadedFoldersDetected` is already a call to action:
    case ShreddingIncomplete(_) => Severity.Warning
    case FolderIsUnloaded(_)    => Severity.Warning
    // errors: these prevent the loader from running whatsoever. The loader will crash and exit after sending these alerts
    case FailedInitialConnection(_)      => Severity.Error
    case FailedToCreateEventsTable(_)    => Severity.Error
    case FailedToCreateManifestTable(_)  => Severity.Error
    case FailedToCreateDatabaseSchema(_) => Severity.Error
  }

  def getMessage(am: Alert): String = {
    val full = am match {
      case FolderIsAlreadyLoaded(_)               => "Folder is already loaded"
      case FolderIsUnloaded(_)                    => "Unloaded batch"
      case ShreddingIncomplete(_)                 => "Incomplete shredding"
      case UnloadedFoldersDetected                => "Folder monitoring detected unloaded folders"
      case UnskippableLoadFailure(_, t)           => show"Load failed and will be retried until fixed: $t"
      case RetryableLoadFailure(_, t)             => show"Load failed and went to the retry queue: $t"
      case TerminalLoadFailure(_, t)              => show"Load failed and will not be retried: $t"
      case FailedInitialConnection(t)             => show"Failed to get connection at startup: $t"
      case FailedToCreateEventsTable(t)           => show"Failed to create events table: $t"
      case FailedToCreateManifestTable(t)         => show"Failed to create manifest table: $t"
      case FailedToCreateDatabaseSchema(t)        => show"Failed to create database schema: $t"
      case FailedHealthCheck(t)                   => show"DB failed health check: $t"
      case FailedFolderMonitoring(t, numFailures) => show"Folder monitoring failed $numFailures times in a row: $t"
    }
    full.take(MaxAlertPayloadLength)
  }

  implicit def throwableShow: Show[Throwable] = {
    def go(acc: List[String], next: Throwable): String = {
      val nextMessage = next match {
        case t: SQLException => Some(s"${t.getMessage} = SqlState: ${t.getSQLState}")
        case t               => Option(t.getMessage)
      }
      val msgs = nextMessage.filterNot(msg => acc.headOption.contains(msg)) ++: acc

      Option(next.getCause) match {
        case Some(cause) => go(msgs, cause)
        case None        => msgs.reverse.mkString(": ")
      }
    }

    Show.show(go(Nil, _))
  }

}
