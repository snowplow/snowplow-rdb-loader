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

import scala.concurrent.ExecutionContext

import cats.data.NonEmptyList
import cats.implicits._

import cats.effect.{Clock, Resource, Timer, ConcurrentEffect, Sync}

import io.circe.Json

import io.sentry.SentryClient

import com.ifountain.opsgenie.client.OpsGenieClient

import org.http4s.client.blaze.BlazeClientBuilder

import com.snowplowanalytics.iglu.core.{SchemaVer, SelfDescribingData, SchemaKey}

import com.snowplowanalytics.snowplow.scalatracker.{Tracker, Emitter}
import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s.Http4sEmitter

import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.common.config.Config
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics.{Metrics, Reporter}

trait Monitoring[F[_]] {

  /** Track result via Snowplow tracker */
  def track(result: Either[LoaderError, Unit]): F[Unit]

  /** Log an error to Sentry if it's configured */
  def trackException(e: Throwable): F[Unit]

  /** Send metrics */
  def reportMetrics(metrics: Metrics.KVMetrics): F[Unit]

  /** Send OpsGenie alerts */
  def alertUnloadedBatches(folders: List[Folder]): F[Unit]

  def alertCorruptedBatches(folders: List[Folder]): F[Unit]

  def alertBadData(folders: List[Folder]): F[Unit]
}

object Monitoring {
  def apply[F[_]](implicit ev: Monitoring[F]): Monitoring[F] = ev

  val LoadSucceededSchema = SchemaKey("com.snowplowanalytics.monitoring.batch", "load_succeeded", "jsonschema", SchemaVer.Full(1,0,0))
  val LoadFailedSchema = SchemaKey("com.snowplowanalytics.monitoring.batch", "load_failed", "jsonschema", SchemaVer.Full(1,0,0))

  def monitoringInterpreter[F[_]: Sync](
    tracker: Option[Tracker[F]],
    sentryClient: Option[SentryClient],
    reporters: List[Reporter[F]],
    opsGenieClient: Option[OpsGenieClient]
  ): Monitoring[F] =
    new Monitoring[F] {

      val _ = opsGenieClient

      /** Track result via Snowplow tracker */
      def track(result: Either[LoaderError, Unit]): F[Unit] =
        trackEmpty(result.fold(_ => LoadFailedSchema, _ => LoadSucceededSchema))

      def trackException(e: Throwable): F[Unit] =
        sentryClient.fold(Sync[F].unit)(s => Sync[F].delay(s.sendException(e)))

      def reportMetrics(metrics: Metrics.KVMetrics): F[Unit] =
        reporters.traverse_(r => r.report(metrics.toList))

      def alertUnloadedBatches(folders: List[Folder]): F[Unit] = Sync[F].unit

      def alertCorruptedBatches(folders: List[Folder]): F[Unit] = Sync[F].unit

      def alertBadData(folders: List[Folder]): F[Unit]= Sync[F].unit

      private def trackEmpty(schema: SchemaKey): F[Unit] =
        tracker match {
          case Some(t) =>
            t.trackSelfDescribingEvent(SelfDescribingData(schema, Json.obj()))
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

