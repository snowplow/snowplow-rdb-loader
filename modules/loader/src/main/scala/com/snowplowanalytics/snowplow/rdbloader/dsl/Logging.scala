/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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

import java.time.{ZoneId, Instant}
import java.time.format.DateTimeFormatter

import scala.util.control.NonFatal

import org.joda.time.DateTime
import cats.Id
import cats.data.NonEmptyList
import cats.implicits._

import cats.effect.Sync
import cats.effect.concurrent.Ref

import io.circe.Json

import com.snowplowanalytics.iglu.core.{SchemaVer, SelfDescribingData, SchemaKey}

import com.snowplowanalytics.snowplow.scalatracker.emitters.id.RequestProcessor._
import com.snowplowanalytics.snowplow.scalatracker.{Tracker, Emitter}
import com.snowplowanalytics.snowplow.scalatracker.emitters.id.SyncBatchEmitter
import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.common.Config.Monitoring
import com.snowplowanalytics.snowplow.rdbloader.common.{Common, _}

import io.sentry.Sentry


trait Logging[F[_]] {

  /** Track result via Snowplow tracker */
  def track(result: Either[LoaderError, Unit]): F[Unit]

  /** Print message to stdout */
  def info(message: String): F[Unit]

  /** Print error message to stderr */
  def error(message: String): F[Unit]

  /** Log an error to Sentry if it's configured */
  def trackException(e: Throwable): F[Unit]
}

object Logging {
  def apply[F[_]](implicit ev: Logging[F]): Logging[F] = ev

  val LoadSucceededSchema = SchemaKey("com.snowplowanalytics.monitoring.batch", "load_succeeded", "jsonschema", SchemaVer.Full(1,0,0))
  val LoadFailedSchema = SchemaKey("com.snowplowanalytics.monitoring.batch", "load_failed", "jsonschema", SchemaVer.Full(1,0,0))

  private val dateFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault())

  def loggingInterpreter[F[_]: Sync](targetConfig: StorageTarget,
                                     messages: Ref[F, List[String]],
                                     tracker: Option[Tracker[Id]]): Logging[F] =
    new Logging[F] {

      /** Track result via Snowplow tracker */
      def track(result: Either[LoaderError, Unit]): F[Unit] = {
        result match {
          case Right(_) =>
            trackEmpty(LoadSucceededSchema)
          case Left(error) =>
            trackEmpty(LoadFailedSchema) *> this.info(error.show)
        }
      }

      /** Print message to stdout */
      def info(message: String): F[Unit] =
        for {
          time <- Sync[F].delay(dateFormatter.format(Instant.now()))
          timestamped = sanitize(s"INFO $time: $message")
          _ <- Sync[F].delay(System.out.println(timestamped)) *> log(timestamped)
        } yield ()

      def error(message: String): F[Unit] =
        for {
          time <- Sync[F].delay(dateFormatter.format(Instant.now()))
          timestamped = sanitize(s"ERROR $time: $message")
          _ <- Sync[F].delay(System.out.println(timestamped)) *> log(timestamped)
        } yield ()

      def trackException(e: Throwable): F[Unit] =
        Sync[F].delay(Sentry.captureException(e)).void.recover {
          case NonFatal(_) => ()
        }

      private def sanitize(string: String): String =
        Common.sanitize(string, List(targetConfig.password.getUnencrypted, targetConfig.username))

      private def log(message: String): F[Unit] =
        messages.update(buf => message :: buf)

      private def trackEmpty(schema: SchemaKey): F[Unit] =
        tracker match {
          case Some(t) =>
            Sync[F].delay(t.trackSelfDescribingEvent(SelfDescribingData(schema, Json.obj())))
          case None =>
            Sync[F].unit
        }
    }

  /**
   * Initialize Snowplow tracker, if `monitoring` section is properly configured
   *
   * @param monitoring config.yml `monitoring` section
   * @return some tracker if enabled, none otherwise
   */
  def initializeTracking[F[_]: Sync](monitoring: Monitoring): F[Option[Tracker[Id]]] =
    monitoring.snowplow.map(_.collector) match {
      case Some(Collector((host, port))) =>
        Sync[F].delay {
          val emitter: Emitter[Id] =
            SyncBatchEmitter.createAndStart(host, port = Some(port), bufferSize = 1, callback = Some(callback))
          val tracker = new Tracker[Id](NonEmptyList.of(emitter), "snowplow-rdb-loader", monitoring.snowplow.map(_.appId).getOrElse("rdb-loader"))
          Some(tracker)
        }
      case Some(_) => Sync[F].pure(none[Tracker[Id]])
      case None => Sync[F].pure(none[Tracker[Id]])
    }

  /** Callback for failed  */
  private def callback(params: CollectorParams, request: CollectorRequest, response: CollectorResponse): Unit = {
    val _ = request
    def toMsg(rsp: CollectorResponse, includeHeader: Boolean): String = rsp match {
      case CollectorFailure(code) =>
        val header = if (includeHeader) { s"Snowplow Tracker [${DateTime.now()}]: " } else ""
        header ++ s"Cannot deliver event to ${params.getUri}. Collector responded with $code"
      case TrackerFailure(error) =>
        val header = if (includeHeader) { s"Snowplow Tracker [${DateTime.now()}]: " } else ""
        header ++ s"Cannot deliver event to ${params.getUri}. Tracker failed due ${error.getMessage}"
      case RetriesExceeded(r) => s"Tracker [${DateTime.now()}]: Gave up on trying to deliver event. Last error: ${toMsg(r, false)}"
      case CollectorSuccess(_) => ""
    }

    val message = toMsg(response, true)

    // The only place in interpreters where println used instead of logger as this is async function
    if (message.isEmpty) () else println(message)
  }


  /**
   * Config helper functions
   */
  private object Collector {
    def isInt(s: String): Boolean = try { s.toInt; true } catch { case _: NumberFormatException => false }

    def unapply(hostPort: String): Option[(String, Int)] =
      hostPort.split(":").toList match {
        case host :: port :: Nil if isInt(port) => Some((host, port.toInt))
        case host :: Nil => Some((host, 80))
        case _ => None
      }
  }
}

