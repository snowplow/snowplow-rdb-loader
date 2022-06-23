/*
 * Copyright (c) 2022-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.telemetry

import scala.concurrent.ExecutionContext

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import cats.data.NonEmptyList
import cats.implicits._

import cats.effect.{ConcurrentEffect, Resource, Sync, Timer}

import fs2.Stream

import org.http4s.client.{Client => HttpClient}
import org.http4s.client.blaze.BlazeClientBuilder

import io.circe.Json
import io.circe.Encoder
import io.circe.syntax._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.scalatracker.Tracker
import com.snowplowanalytics.snowplow.scalatracker.Emitter._
import com.snowplowanalytics.snowplow.scalatracker.Emitter.{Result => TrackerResult}
import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s.Http4sEmitter

import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.Config.{Telemetry => TelemetryConfig}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.Config
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.Config.StreamInput.Kinesis

trait Telemetry[F[_]] {
  def report: Stream[F, Unit]
}

object Telemetry {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def build[F[_]: ConcurrentEffect: Timer](
    config: Config,
    appName: String,
    appVersion: String,
    executionContext: ExecutionContext
  ): Resource[F, Telemetry[F]] =
    for {
      httpClient <- BlazeClientBuilder[F](executionContext).resource
      tracker <- initTracker(config.telemetry, appName, httpClient)
    } yield new Telemetry[F] {
      def report: Stream[F, Unit] =
        if (config.telemetry.disable) {
          Stream.empty.covary[F]
        } else {
          val sdj = makeHeartbeatEvent(
            config.telemetry,
            getRegionFromConfig(config),
            getCloudFromConfig(config),
            appName,
            appVersion
          )
          Stream
            .fixedDelay[F](config.telemetry.interval)
            .evalMap { _ =>
              tracker.trackSelfDescribingEvent(unstructEvent = sdj) >>
                tracker.flushEmitters()
            }
        }
    }

  private def initTracker[F[_]: ConcurrentEffect: Timer](
    config: TelemetryConfig,
    appName: String,
    client: HttpClient[F]
  ): Resource[F, Tracker[F]] =
    for {
      emitter <- Http4sEmitter.build(
                   EndpointParams(config.collectorUri, port = Some(config.collectorPort), https = config.secure),
                   client,
                   retryPolicy = RetryPolicy.MaxAttempts(10),
                   callback = Some(emitterCallback[F] _)
                 )
    } yield new Tracker(NonEmptyList.of(emitter), "tracker-telemetry", appName)

  private def emitterCallback[F[_]: Sync](
    params: EndpointParams,
    req: Request,
    res: TrackerResult
  ): F[Unit] =
    res match {
      case TrackerResult.Success(_) =>
        Logger[F].debug(s"Telemetry heartbeat successfully sent to ${params.getGetUri}")
      case TrackerResult.Failure(code) =>
        Logger[F].warn(s"Sending telemetry hearbeat got unexpected HTTP code $code from ${params.getUri}")
      case TrackerResult.TrackerFailure(exception) =>
        Logger[F].warn(
          s"Telemetry hearbeat failed to reach ${params.getUri} with following exception $exception after ${req.attempt} attempts"
        )
      case TrackerResult.RetriesExceeded(failure) =>
        Logger[F].error(s"Stopped trying to send telemetry heartbeat after following failure: $failure")
    }

  private def makeHeartbeatEvent(
    teleCfg: TelemetryConfig,
    region: Option[String],
    cloud: Option[Cloud],
    appName: String,
    appVersion: String
  ): SelfDescribingData[Json] =
    SelfDescribingData(
      SchemaKey("com.snowplowanalytics.oss", "oss_context", "jsonschema", SchemaVer.Full(1, 0, 1)),
      Json.obj(
        "userProvidedId" -> teleCfg.userProvidedId.asJson,
        "autoGeneratedId" -> teleCfg.autoGeneratedId.asJson,
        "moduleName" -> teleCfg.moduleName.asJson,
        "moduleVersion" -> teleCfg.moduleVersion.asJson,
        "instanceId" -> teleCfg.instanceId.asJson,
        "appGeneratedId" -> com.snowplowanalytics.snowplow.rdbloader.transformer.AppId.appId.asJson,
        "cloud" -> cloud.asJson,
        "region" -> region.asJson,
        "applicationName" -> appName.asJson,
        "applicationVersion" -> appVersion.asJson
      )
    )

  private def getRegionFromConfig(config: Config): Option[String] =
    config.input match {
      case c: Kinesis => Some(c.region.name)
      case _ => None
    }

  private def getCloudFromConfig(config: Config): Option[Cloud] =
    config.input match {
      case _: Kinesis => Some(Cloud.Aws)
      case _ => None
    }

  sealed trait Cloud
  object Cloud {
    case object Aws extends Cloud

    implicit val encoder: Encoder[Cloud] = Encoder.encodeString.contramap[Cloud](_.toString.toUpperCase)
  }
}

