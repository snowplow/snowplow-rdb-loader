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

import java.time.Instant
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect.{Resource, Sync}
import cats.effect.kernel.Async
import cats.effect.std.Random
import io.circe._
import io.circe.generic.semiauto._
import org.http4s.{EntityDecoder, EntityEncoder, Method, Request}
import org.http4s.client.Client
import org.http4s.circe.jsonEncoderOf
import io.sentry.SentryClient
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.snowplow.scalatracker.{Emitter, Tracker, Tracking}
import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s.Http4sEmitter
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics.Metrics.PeriodicMetrics
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics.{Metrics, Reporter}
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.LoadSuccess
import org.http4s.FormDataDecoder.formEntityDecoder

trait Monitoring[F[_]] { self =>

  /** Log an error to Sentry if it's configured */
  def trackException(e: Throwable): F[Unit]

  /** Send metrics */
  def reportMetrics(metrics: Metrics.KVMetrics): F[Unit]

  /** Track all details about loaded folder */
  def success(payload: Monitoring.SuccessPayload): F[Unit]

  def periodicMetrics: Metrics.PeriodicMetrics[F]

  /**
   * Send an event with `iglu:com.snowplowanalytics.monitoring.batch/alert/jsonschema/1-0-0` to
   * either HTTP webhook endpoint or snowplow collector, whichever is configured (can be both)
   */
  def alert(message: Alert): F[Unit]

}

object Monitoring {

  private implicit val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  def apply[F[_]](implicit ev: Monitoring[F]): Monitoring[F] = ev

  val LoadSucceededSchema = SchemaKey("com.snowplowanalytics.monitoring.batch", "load_succeeded", "jsonschema", SchemaVer.Full(3, 0, 1))
  val AlertSchema         = SchemaKey("com.snowplowanalytics.monitoring.batch", "alert", "jsonschema", SchemaVer.Full(1, 0, 0))

  val Application: String =
    s"snowplow-rdb-loader-${BuildInfo.version}"

  final case class AlertPayload(
    application: String,
    base: Option[BlobStorage.Folder],
    severity: AlertPayload.Severity,
    message: String,
    tags: Map[String, String]
  )

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

    def toSelfDescribing(payload: AlertPayload): SelfDescribingData[Json] =
      SelfDescribingData(AlertSchema, derivedEncoder.apply(payload))

    def ofAlert(am: Alert): AlertPayload =
      AlertPayload(Application, Alert.getFolder(am), Alert.getSeverity(am), Alert.getMessage(am), Map.empty)

    implicit val alertPayloadEncoder: Encoder[AlertPayload] =
      Encoder[Json].contramap[AlertPayload](p => toSelfDescribing(p).normalize)

    implicit def alertPayloadEntityEncoder[F[_]]: EntityEncoder[F, AlertPayload] =
      jsonEncoderOf[F, AlertPayload]
  }

  final case class SuccessPayload(
    shredding: ShreddingComplete,
    application: String,
    attempt: Int,
    loadingStarted: Instant,
    loadingCompleted: Instant,
    recoveryTableNames: Option[List[String]],
    tags: Map[String, String]
  )

  object SuccessPayload {
    // Very odd hack, but I couldn't derive a right codec without it
    // We should get rid of single-leaf ADT
    private[dsl] implicit val shreddingCompleteEncoder: Encoder[ShreddingComplete] =
      loaderMessageShreddingCompleteEncoder.contramap { e: ShreddingComplete => e }

    private val derivedEncoder: Encoder[SuccessPayload] =
      deriveEncoder[SuccessPayload]

    def toSelfDescribing(success: SuccessPayload): SelfDescribingData[Json] =
      SelfDescribingData(LoadSucceededSchema, derivedEncoder.apply(success))

    implicit val successPayloadEncoder: Encoder[SuccessPayload] =
      Encoder[Json].contramap[SuccessPayload](p => toSelfDescribing(p).normalize)

    implicit def successPayloadEntityEncoder[F[_]]: EntityEncoder[F, SuccessPayload] =
      jsonEncoderOf[F, SuccessPayload]

    def build(
      shredding: ShreddingComplete,
      attempts: Int,
      start: Instant,
      ingestion: Instant,
      loadResult: LoadSuccess
    ): SuccessPayload = {
      val tableNames = if (loadResult.recoveryTableNames.isEmpty) None else loadResult.recoveryTableNames.some
      SuccessPayload(shredding, Application, attempts, start, ingestion, tableNames, Map.empty)
    }
  }

  def monitoringInterpreter[F[_]: Sync: Logging](
    tracker: Option[Tracker[F]],
    sentryClient: Option[SentryClient],
    reporters: List[Reporter[F]],
    webhookConfig: Option[Config.Webhook],
    httpClient: Client[F],
    pm: PeriodicMetrics[F],
    reportRecoveryTableMetrics: Boolean
  )(implicit E: EntityDecoder[F, String]
  ): Monitoring[F] =
    new Monitoring[F] {

      def viaWebhook[A: EntityEncoder[F, *]](payload: A, addTags: (A, Config.Webhook) => A): Option[F[Unit]] =
        webhookConfig match {
          case Some(webhook) =>
            val request: Request[F] =
              Request[F](Method.POST, webhook.endpoint)
                .withEntity(addTags(payload, webhook))

            val req = httpClient
              .run(request)
              .use { response =>
                if (response.status.isSuccess) Sync[F].unit
                else
                  response.as[String].flatMap(body => Logging[F].error(s"Webhook ${webhook.endpoint} returned non-2xx response:\n$body"))
              }
              .handleErrorWith { e =>
                Logging[F].error(e)(s"Webhook ${webhook.endpoint} resulted in exception without a response")
              }
            Some(req)
          case None =>
            None
        }

      def trackException(e: Throwable): F[Unit] =
        sentryClient.fold(Sync[F].unit)(s => Sync[F].delay(s.sendException(e)))

      def reportMetrics(metrics: Metrics.KVMetrics): F[Unit] =
        reporters.traverse_(r => r.report(metrics.toList(reportRecoveryTableMetrics)))

      def success(payload: SuccessPayload): F[Unit] = {
        val webhookRequest = viaWebhook[SuccessPayload](payload, (p, c) => p.copy(tags = p.tags ++ c.tags)) match {
          case Some(req) => req
          case None      => Logging[F].debug("Webhook monitoring is not configured, skipping success tracking")
        }

        val snowplowRequest = tracker match {
          case Some(t) => t.trackSelfDescribingEvent(SuccessPayload.toSelfDescribing(payload))
          case None    => Logging[F].debug("Snowplow monitoring is not configured, skipping success tracking")
        }

        snowplowRequest *> webhookRequest
      }

      def periodicMetrics: Metrics.PeriodicMetrics[F] = pm

      def alert(message: Alert): F[Unit] = {
        val payload = AlertPayload.ofAlert(message)
        val webhookRequest = viaWebhook[AlertPayload](payload, (p, c) => p.copy(tags = p.tags ++ c.tags)) match {
          case Some(req) => req
          case None      => Logging[F].debug("Webhook monitoring is not configured, skipping alert")
        }

        val snowplowRequest = tracker match {
          case Some(t) => t.trackSelfDescribingEvent(AlertPayload.toSelfDescribing(payload))
          case None    => Logging[F].debug("Snowplow monitoring is not configured, skipping alert")
        }

        snowplowRequest *> webhookRequest
      }
    }

  /**
   * Initialize Snowplow tracker, if `monitoring` section is properly configured
   *
   * @param monitoring
   *   config.yml `monitoring` section
   * @return
   *   some tracker if enabled, none otherwise
   */
  def initializeTracking[F[_]: Async: Random: Tracking: Logging](
    monitoring: Config.Monitoring,
    client: Client[F]
  ): Resource[F, Option[Tracker[F]]] =
    monitoring.snowplow.map(_.collector) match {
      case Some(Collector((host, port))) =>
        val endpoint = Emitter.EndpointParams(host, Some(port), port == 443)
        Http4sEmitter.build[F](endpoint, client, callback = Some(callback[F])).map { emitter =>
          Some(new Tracker[F](NonEmptyList.of(emitter), "snowplow-rdb-loader", monitoring.snowplow.map(_.appId).getOrElse("rdb-loader")))
        }
      case None => Resource.pure[F, Option[Tracker[F]]](none[Tracker[F]])
    }

  /** Callback for failed */
  private def callback[F[_]: Sync: Logging](
    params: Emitter.EndpointParams,
    request: Emitter.Request,
    response: Emitter.Result
  ): F[Unit] = {
    val _ = request
    def toMsg(rsp: Emitter.Result): Option[String] = rsp match {
      case Emitter.Result.Failure(code) =>
        s"Cannot deliver event to ${params.getUri}. Collector responded with $code".some
      case Emitter.Result.TrackerFailure(error) =>
        s"Cannot deliver event to ${params.getUri}. Tracker failed due ${error.getMessage}".some
      case Emitter.Result.RetriesExceeded(_) =>
        s"Tracker gave up on trying to deliver event".some
      case Emitter.Result.Success(_) =>
        none[String]
    }

    toMsg(response) match {
      case Some(msg) => Logging[F].warning(msg)
      case None      => Logging[F].debug("Snowplow event has been submitted")
    }
  }

  /**
   * Config helper functions
   */
  private object Collector {
    def isInt(s: String): Boolean = try { s.toInt; true }
    catch { case _: NumberFormatException => false }

    def unapply(hostPort: String): Option[(String, Int)] =
      hostPort.split(":").toList match {
        case host :: port :: Nil if isInt(port) => Some((host, port.toInt))
        case host :: Nil                        => Some((host, 80))
        case _                                  => None
      }
  }
}
