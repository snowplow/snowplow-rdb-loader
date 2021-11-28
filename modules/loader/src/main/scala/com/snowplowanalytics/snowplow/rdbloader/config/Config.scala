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
package com.snowplowanalytics.snowplow.rdbloader.config

import java.net.URI

import scala.concurrent.duration.{Duration, FiniteDuration}

import cats.effect.Sync
import cats.data.EitherT
import cats.syntax.either._

import io.circe._
import io.circe.generic.semiauto._

import org.http4s.{ParseFailure, Uri}

import cron4s.CronExpr
import cron4s.circe._

import com.snowplowanalytics.snowplow.rdbloader.config.Config._
import com.snowplowanalytics.snowplow.rdbloader.common.config.{Step, ConfigUtils, Region}
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.common._

/**
 * Main config file parsed from HOCON
 * @tparam D kind of supported warehouse
 */
final case class Config[+D <: StorageTarget](region: Region,
                                             jsonpaths: Option[S3.Folder],
                                             monitoring: Monitoring,
                                             messageQueue: String,
                                             storage: D,
                                             steps: Set[Step],
                                             schedules: Schedules)

object Config {

  val MetricsDefaultPrefix = "snowplow.rdbloader"

  def fromString[F[_]: Sync](s: String): EitherT[F, String, Config[StorageTarget]] =
    fromString(s, implicits().configDecoder)

  def fromString[F[_]: Sync](s: String, configDecoder: Decoder[Config[StorageTarget]]): EitherT[F, String, Config[StorageTarget]] =  {
    implicit val implConfigDecoder: Decoder[Config[StorageTarget]] = configDecoder
    ConfigUtils.fromStringF[F, Config[StorageTarget]](s)
  }

  final case class Schedule(name: String, when: CronExpr, duration: FiniteDuration)
  final case class Schedules(noOperation: List[Schedule])
  final case class Monitoring(snowplow: Option[SnowplowMonitoring], sentry: Option[Sentry], metrics: Option[Metrics], webhook: Option[Webhook], folders: Option[Folders])
  final case class SnowplowMonitoring(appId: String, collector: String)
  final case class Sentry(dsn: URI)
  final case class Metrics(statsd: Option[StatsD], stdout: Option[Stdout])
  final case class StatsD(hostname: String, port: Int, tags: Map[String, String], prefix: Option[String])
  final case class Stdout(prefix: Option[String])
  final case class Webhook(endpoint: Uri, tags: Map[String, String])
  final case class Folders(period: FiniteDuration, staging: S3.Folder, since: Option[FiniteDuration], shredderOutput: S3.Folder, until: Option[FiniteDuration])


  /**
   * All config implicits are put into case class because we want to make region decoder
   * replaceable to write unit tests for config parsing.
   */
  final case class implicits(regionConfigDecoder: Decoder[Region] = Region.regionConfigDecoder) {
    implicit val implRegionConfigDecoder: Decoder[Region] =
      regionConfigDecoder

    implicit val snowplowMonitoringDecoder: Decoder[SnowplowMonitoring] =
      deriveDecoder[SnowplowMonitoring]

    implicit val sentryDecoder: Decoder[Sentry] =
      deriveDecoder[Sentry]

    implicit val periodicDurationDecoder: Decoder[Schedule] =
      deriveDecoder[Schedule]

    implicit val schedulesDecoder: Decoder[Schedules] =
      deriveDecoder[Schedules]

    implicit val statsdDecoder: Decoder[StatsD] =
      deriveDecoder[StatsD]

    implicit val stdoutDecoder: Decoder[Stdout] =
      deriveDecoder[Stdout]

    implicit val metricsDecoder: Decoder[Metrics] =
      deriveDecoder[Metrics]

    implicit val http4sUriDecoder: Decoder[Uri] =
      Decoder[String].emap(s => Either.catchOnly[ParseFailure](Uri.unsafeFromString(s)).leftMap(_.toString))

    implicit val minuteDecoder: Decoder[FiniteDuration] =
      Decoder[String].emap { str =>
        Either
          .catchOnly[NumberFormatException](Duration.create(str))
          .leftMap(_.toString)
          .flatMap { duration =>
            if (duration.isFinite) Right(duration.asInstanceOf[FiniteDuration])
            else Left(s"Cannot convert Duration $duration to FiniteDuration")
          }
      }

    implicit val uriDecoder: Decoder[URI] =
      Decoder[String].emap(s => Either.catchOnly[IllegalArgumentException](URI.create(s)).leftMap(_.toString))

    implicit val webhookDecoder: Decoder[Webhook] =
      deriveDecoder[Webhook]
    implicit val foldersDecoder: Decoder[Folders] =
      deriveDecoder[Folders]

    implicit val monitoringDecoder: Decoder[Monitoring] =
      deriveDecoder[Monitoring]

    implicit val configDecoder: Decoder[Config[StorageTarget]] =
      deriveDecoder[Config[StorageTarget]]
  }
}
