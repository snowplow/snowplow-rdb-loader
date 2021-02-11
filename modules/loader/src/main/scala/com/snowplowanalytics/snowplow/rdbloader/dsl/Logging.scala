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

import org.joda.time.DateTime

import cats.{Id, Monad}
import cats.data.NonEmptyList
import cats.syntax.option._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.either._
import cats.syntax.functor._
import cats.syntax.show._
import cats.instances.either._

import cats.effect.Sync
import cats.effect.concurrent.Ref

import io.circe.Json

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.scalatracker.emitters.id.RequestProcessor._
import com.snowplowanalytics.snowplow.scalatracker.{Emitter, Tracker}
import com.snowplowanalytics.snowplow.scalatracker.emitters.id.{SyncBatchEmitter, SyncEmitter}

import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.common._
import com.snowplowanalytics.snowplow.rdbloader.config.SnowplowConfig.{GetMethod, PostMethod}
import com.snowplowanalytics.snowplow.rdbloader.config.SnowplowConfig.Monitoring
import com.snowplowanalytics.snowplow.rdbloader.utils.{Common, S3}


trait Logging[F[_]] {

  /** Get last COPY statement in case of failure */
  def getLastCopyStatements: F[String]

  /** Track result via Snowplow tracker */
  def track(result: Either[LoaderError, Unit]): F[Unit]

  /** Dump log to S3 */
  def dump(key: S3.Key)(implicit S: AWS[F]): F[Either[String, S3.Key]]

  /** Print message to stdout */
  def print(message: String): F[Unit]
}

object Logging {
  def apply[F[_]](implicit ev: Logging[F]): Logging[F] = ev

  val ApplicationContextSchema = SchemaKey("com.snowplowanalytics.monitoring.batch", "application_context", "jsonschema", SchemaVer.Full(1,0,0))
  val LoadSucceededSchema = SchemaKey("com.snowplowanalytics.monitoring.batch", "load_succeeded", "jsonschema", SchemaVer.Full(1,0,0))
  val LoadFailedSchema = SchemaKey("com.snowplowanalytics.monitoring.batch", "load_failed", "jsonschema", SchemaVer.Full(1,0,0))

  def controlInterpreter[F[_]: Sync](targetConfig: StorageTarget,
                                     messages: Ref[F, List[String]],
                                     tracker: Option[Tracker[Id]]): Logging[F] =
    new Logging[F] {

      def getLastCopyStatements: F[String] =
        messages.get.map(_.find(_.startsWith("COPY ")).getOrElse("No COPY statements performed"))

      /** Track result via Snowplow tracker */
      def track(result: Either[LoaderError, Unit]): F[Unit] = {
        result match {
          case Right(_) =>
            trackEmpty(LoadSucceededSchema)
          case Left(error) =>
            val secrets = List(targetConfig.password.getUnencrypted, targetConfig.username)
            val sanitizedMessage = Common.sanitize(error.show, secrets)
            trackEmpty(LoadFailedSchema) *> this.print(sanitizedMessage)
        }
      }

      /** Dump log to S3 */
      def dump(key: S3.Key)(implicit S: AWS[F]): F[Either[String, S3.Key]] =
        log(s"Dumping $key") *>
          Monad[F].ifM(S.keyExists(key))(
            Monad[F].pure(Left(s"S3 log object [$key] already exists")),
            for {
              logs <- getMessages.map(_.mkString("\n") + "\n")
              putResult <- S.putObject(key, logs).value
            } yield putResult.as(key).leftMap(_.show)
          )


      /** Print message to stdout */
      def print(message: String): F[Unit] =
        for {
          time <- Sync[F].delay(DateTime.now())
          timestamped = s"RDB Loader [$time]: $message"
          _ <- Sync[F].delay(System.out.println(timestamped)) *> log(timestamped)
        } yield ()

      private def log(message: String): F[Unit] =
        messages.update(buf => message :: buf)

      private def trackEmpty(schema: SchemaKey): F[Unit] =
        tracker match {
          case Some(t) =>
            Sync[F].delay(t.trackSelfDescribingEvent(SelfDescribingData(schema, Json.obj())))
          case None =>
            Sync[F].unit
        }

      private def getMessages: F[List[String]] =
        messages.get.map(_.reverse)
    }

  /**
   * Initialize Snowplow tracker, if `monitoring` section is properly configured
   *
   * @param monitoring config.yml `monitoring` section
   * @return some tracker if enabled, none otherwise
   */
  def initializeTracking[F[_]: Sync](monitoring: Monitoring): F[Option[Tracker[Id]]] = {
    monitoring.snowplow.flatMap(_.collector) match {
      case Some(Collector((host, port))) =>
        Sync[F].delay {
          val emitter: Emitter[Id] = monitoring.snowplow.flatMap(_.method) match {
            case Some(GetMethod) =>
              SyncEmitter.createAndStart(host, port = Some(port), callback = Some(callback))
            case Some(PostMethod) =>
              SyncBatchEmitter.createAndStart(host, port = Some(port), bufferSize = 2)
            case None =>
              SyncEmitter.createAndStart(host, port = Some(port), callback = Some(callback))
          }

          val tracker = new Tracker[Id](NonEmptyList.of(emitter), "snowplow-rdb-loader", monitoring.snowplow.flatMap(_.appId).getOrElse("rdb-loader"))
          Some(tracker)
        }
      case Some(_) => Sync[F].pure(none[Tracker[Id]])
      case None => Sync[F].pure(none[Tracker[Id]])
    }
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

