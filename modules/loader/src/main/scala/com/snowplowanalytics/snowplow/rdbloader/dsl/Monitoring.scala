/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect.{Blocker, Clock, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.Parallel

import fs2.Stream

import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._

import org.http4s.{Method, Uri}
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.Client
import org.http4s.headers.`Content-Type`
import org.http4s.{MediaType, Request}

import scala.concurrent.ExecutionContext

import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.scalatracker.{Emitter, Tracker}
import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s.Http4sEmitter

import io.sentry.SentryClient

import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.common.config.Config
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.rdbloader.common.config.Config.Webhook
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics.{Metrics, Reporter}
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo

trait Monitoring[F[_]] {

  /** Track result via Snowplow tracker */
  def track(result: Either[LoaderError, Unit]): F[Unit]

  /** Log an error to Sentry if it's configured */
  def trackException(e: Throwable): F[Unit]

  /** Send metrics */
  def reportMetrics(metrics: Metrics.KVMetrics): F[Unit]

  /** Send OpsGenie alert */
  def alert(alert: Alert): F[Unit]
}

sealed trait Alert
case class UnloadedBatchAlert(folders: List[Folder]) extends Alert
case class CorruptedBatchAlert(folders: List[Folder]) extends Alert

object Monitoring {
  def apply[F[_]](implicit ev: Monitoring[F]): Monitoring[F] = ev

  val LoadSucceededSchema = SchemaKey("com.snowplowanalytics.monitoring.batch", "load_succeeded", "jsonschema", SchemaVer.Full(1,0,0))
  val LoadFailedSchema = SchemaKey("com.snowplowanalytics.monitoring.batch", "load_failed", "jsonschema", SchemaVer.Full(1,0,0))

  val AlertSchemaKey = SchemaKey("com.snowplowanalytics.snowplow.storage.rdbloader", "alert", "jsonschema", SchemaVer.Full(1, 0, 0))

  case class AlertPayload(loaderVersion: String, folderName: String, message: String, tags: Map[String, String])

  implicit val AlertPayloadEncoder: Encoder[AlertPayload] = deriveEncoder[AlertPayload]

  def monitoringInterpreter[F[_]: ContextShift: Sync: Parallel](
    tracker: Option[Tracker[F]],
    sentryClient: Option[SentryClient],
    reporters: List[Reporter[F]],
    optWebhook: Option[Webhook],
    httpClient: Client[F],
    blocker: Blocker
  ): Monitoring[F] =
    new Monitoring[F] {

      /** Track result via Snowplow tracker */
      def track(result: Either[LoaderError, Unit]): F[Unit] =
        trackEmpty(result.fold(_ => LoadFailedSchema, _ => LoadSucceededSchema))

      def trackException(e: Throwable): F[Unit] =
        sentryClient.fold(Sync[F].unit)(s => Sync[F].delay(s.sendException(e)))

      def reportMetrics(metrics: Metrics.KVMetrics): F[Unit] =
        reporters.traverse_(r => r.report(metrics.toList))

      private def trackEmpty(schema: SchemaKey): F[Unit] =
        tracker match {
          case Some(t) =>
            t.trackSelfDescribingEvent(SelfDescribingData(schema, Json.obj()))
          case None =>
            Sync[F].unit
        }

      private def postPayload(payload: AlertPayload): String =
        SelfDescribingData[Json](AlertSchemaKey, payload.asJson).normalize.noSpaces

      private def createAlertPayloads(folders: List[Folder], message: String, tags: Map[String, String]): List[AlertPayload] =
        folders.map(folder => AlertPayload(BuildInfo.version, folder.folderName, message, tags))

      /** Send alert payloads to the webhook endpoint provided in config */
      override def alert(alert: Alert): F[Unit] =
        optWebhook.fold(Sync[F].unit){ webhook =>
          val payloads = alert match {
            case UnloadedBatchAlert(folders) => createAlertPayloads(folders, "Unloaded Batch", webhook.tags)
            case CorruptedBatchAlert(folders) => createAlertPayloads(folders, "Corrupted Batch", webhook.tags)
          }
          blocker.blockOn(
            payloads
              .map(p => postPayload(p).getBytes)
              .map(bytes => Stream.emit(bytes).covary[F])
              .map{ body =>
                Request[F](Method.POST, Uri.unsafeFromString(webhook.endpoint))
                  .withEntity(body)
                  .withContentType(`Content-Type`(MediaType.application.json))
              }
              .parTraverse(httpClient.status)
              .void
          )
        }
    }

  /**
   * Initialize Snowplow tracker, if `monitoring` section is properly configured
   *
   * @param monitoring config.yml `monitoring` section
   * @return some tracker if enabled, none otherwise
   */
  def initializeTracking[F[_]: ConcurrentEffect: Timer: Clock](monitoring: Config.Monitoring, ec: ExecutionContext): Resource[F, Option[Tracker[F]]] =
    monitoring.snowplow.map(_.collector) match {
      case Some(Collector((host, port))) =>
        val endpoint = Emitter.EndpointParams(host, Some(port), port == 443)
        for {
          client <- BlazeClientBuilder[F](ec).resource
          emitter <- Http4sEmitter.build[F](endpoint, client, callback = Some(callback[F]))
          tracker = new Tracker[F](NonEmptyList.of(emitter), "snowplow-rdb-loader", monitoring.snowplow.map(_.appId).getOrElse("rdb-loader"))
        } yield Some(tracker)
      case None => Resource.pure[F, Option[Tracker[F]]](none[Tracker[F]])
    }

  /** Callback for failed  */
  private def callback[F[_]: Sync: Clock](params: Emitter.EndpointParams, request: Emitter.Request, response: Emitter.Result): F[Unit] = {
    val _ = request
    def toMsg(rsp: Emitter.Result): String = rsp match {
      case Emitter.Result.Failure(code) =>
        s"Cannot deliver event to ${params.getUri}. Collector responded with $code"
      case Emitter.Result.TrackerFailure(error) =>
        s"Cannot deliver event to ${params.getUri}. Tracker failed due ${error.getMessage}"
      case Emitter.Result.RetriesExceeded(_) =>
        s"Tracker gave up on trying to deliver event"
      case Emitter.Result.Success(_) =>
        ""
    }

    val message = toMsg(response)

    // The only place in interpreters where println used instead of logger as this is async function
    if (message.isEmpty) Sync[F].unit else Sync[F].delay(System.out.println(message))
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

