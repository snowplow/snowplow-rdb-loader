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
import cats.effect.{Clock, ConcurrentEffect, Resource, Sync, Timer}
import io.circe._
import io.circe.generic.semiauto._
import org.http4s.{EntityEncoder, Method, Request}
import org.http4s.client.Client
import org.http4s.circe.jsonEncoderOf
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.snowplow.scalatracker.{Emitter, Tracker}
import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s.Http4sEmitter
import io.sentry.SentryClient
import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics.{Metrics, Reporter}

trait Monitoring[F[_]] {

  /** Track result via Snowplow tracker */
  def track(result: Either[LoaderError, Unit]): F[Unit]

  /** Log an error to Sentry if it's configured */
  def trackException(e: Throwable): F[Unit]

  /** Send metrics */
  def reportMetrics(metrics: Metrics.KVMetrics): F[Unit]

  /** Send an alert to a HTTP endpoint */
  def alert(payload: Monitoring.AlertPayload): F[Unit]

  /** Helper method specifically for exceptions */
  def alert(error: Throwable, folder: S3.Folder): F[Unit] = {
    val message = Option(error.getMessage).getOrElse(error.toString)
    // Note tags are added by Monitoring later
    val payload = Monitoring.AlertPayload(Monitoring.Application, Some(folder), Monitoring.AlertPayload.Severity.Error, message, Map.empty)
    alert(payload)
  }
}

object Monitoring {
  def apply[F[_]](implicit ev: Monitoring[F]): Monitoring[F] = ev

  val LoadSucceededSchema = SchemaKey("com.snowplowanalytics.monitoring.batch", "load_succeeded", "jsonschema", SchemaVer.Full(1,0,0))
  val LoadFailedSchema = SchemaKey("com.snowplowanalytics.monitoring.batch", "load_failed", "jsonschema", SchemaVer.Full(1,0,0))
  val AlertSchema = SchemaKey("com.snowplowanalytics.monitoring.batch", "alert", "jsonschema", SchemaVer.Full(1, 0, 0))

  val Application: String =
    s"snowplow-rdb-loader-${BuildInfo.version}"

  final case class AlertPayload(application: String,
                                base: Option[S3.Folder],
                                severity: AlertPayload.Severity,
                                message: String,
                                tags: Map[String, String])

  object AlertPayload {
    sealed trait Severity
    object Severity {
      final case object Info extends Severity
      final case object Warning extends Severity
      final case object Error extends Severity

      implicit val severityEncoder: Encoder[Severity] =
        Encoder[String].contramap[Severity](_.toString.toUpperCase)
    }

    private val derivedEncoder: Encoder[AlertPayload] =
      deriveEncoder[AlertPayload]

    implicit val alertPayloadEncoder: Encoder[AlertPayload] =
      Encoder[Json].contramap { alert: AlertPayload =>
        SelfDescribingData(AlertSchema, derivedEncoder.apply(alert)).normalize
      }

    implicit def alertPayloadEntityEncoder[F[_]]: EntityEncoder[F, AlertPayload] =
      jsonEncoderOf[F, AlertPayload]

    def info(message: String, folder: S3.Folder): AlertPayload =
      AlertPayload(Application, Some(folder), Severity.Info, message, Map.empty)

    def warn(message: String): AlertPayload =
      AlertPayload(Application, None, Severity.Warning, message, Map.empty)

    def warn(message: String, folder: S3.Folder): AlertPayload =
      AlertPayload(Application, Some(folder), Severity.Warning, message, Map.empty)

    def error(message: String): AlertPayload =
      AlertPayload(Application, None, Severity.Error, message, Map.empty)
  }

  def monitoringInterpreter[F[_]: Sync: Logging](
    tracker: Option[Tracker[F]],
    sentryClient: Option[SentryClient],
    reporters: List[Reporter[F]],
    webhookConfig: Option[Config.Webhook],
    httpClient: Client[F]
  ): Monitoring[F] =
    new Monitoring[F] {

      /** Track result via Snowplow tracker */
      def track(result: Either[LoaderError, Unit]): F[Unit] =
        trackEmpty(result.fold(_ => LoadFailedSchema, _ => LoadSucceededSchema))

      def trackException(e: Throwable): F[Unit] =
        sentryClient.fold(Sync[F].unit)(s => Sync[F].delay(s.sendException(e)))

      def reportMetrics(metrics: Metrics.KVMetrics): F[Unit] =
        reporters.traverse_(r => r.report(metrics.toList))

      def alert(payload: AlertPayload): F[Unit] =
        webhookConfig match {
          case Some(webhook) =>
            val request: Request[F] =
              Request[F](Method.POST, webhook.endpoint)
                .withEntity(payload.copy(tags = payload.tags ++ webhook.tags))

            httpClient
              .run(request)
              .use { response =>
                if (response.status.isSuccess) Sync[F].unit
                else response.as[String].flatMap(body => Logging[F].error(s"Webhook ${webhook.endpoint} returned non-2xx response:\n$body"))
              }
          case None => Sync[F].unit
        }

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
  def initializeTracking[F[_]: ConcurrentEffect: Timer: Clock](monitoring: Config.Monitoring, client: Client[F]): Resource[F, Option[Tracker[F]]] =
    monitoring.snowplow.map(_.collector) match {
      case Some(Collector((host, port))) =>
        val endpoint = Emitter.EndpointParams(host, Some(port), port == 443)
        Http4sEmitter.build[F](endpoint, client, callback = Some(callback[F])).map { emitter =>
          Some(new Tracker[F](NonEmptyList.of(emitter), "snowplow-rdb-loader", monitoring.snowplow.map(_.appId).getOrElse("rdb-loader")))
        }
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

