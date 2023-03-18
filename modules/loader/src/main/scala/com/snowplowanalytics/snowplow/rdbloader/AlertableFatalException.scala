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
package com.snowplowanalytics.snowplow.rdbloader

import cats.Show
import cats.implicits._

/**
 * An exception that is compatible with how use the monitoring webhook.
 *
 * If the app catches this type of exception, then it must either:
 *   - Send an alert, and then handle the exception
 *   - Re-throw the exception, so it can be handled higher up the stack
 *
 * This is the only exception for which it is acceptable to send a monitoring webhook. Other caught
 * exceptions should go to sentry only, as they do provide helpful detail for the pipeline operator.
 */
class AlertableFatalException(prefix: AlertableFatalException.Explanation, cause: Throwable) extends Exception(prefix.show, cause) {

  /**
   * The message that appears in the alert sent to the webhook.
   *
   * The message comprises:
   *   - A controlled statement saying where the error occurred.
   *   - An un-controlled message taken from the underlying exception.
   */
  def alertMessage: String = {
    val suffix = cause match {
      case ae: AlertableFatalException => ae.alertMessage
      case other =>
        Option(other.getMessage).getOrElse("")
    }
    show"$prefix: $suffix"
  }
}

object AlertableFatalException {
  sealed trait Explanation

  /**
   * A controlled set of terms for which the pipeline operator should receive an error alert.
   *
   * Items should appear on this list if it requies action on the warehouse to fix the problem. For
   * example, by fixing permissions or credentials.
   */
  object Explanation {
    case object ManifestInit extends Explanation
    case object InitialConnection extends Explanation
    case object EventsTableInit extends Explanation
    case object TargetQueryInit extends Explanation

    implicit val show: Show[Explanation] =
      Show.show {
        case ManifestInit => "Could not initialize manifest table"
        case EventsTableInit => "Could not initialize events table"
        case InitialConnection => "Could not get connection to warehouse during startup"
        case TargetQueryInit => "Could not acquire information necessary for startup"
      }
  }
}
