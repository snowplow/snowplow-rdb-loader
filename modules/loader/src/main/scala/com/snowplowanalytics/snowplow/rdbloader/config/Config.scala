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
import cats.syntax.option._

import io.circe._
import io.circe.generic.semiauto._

import org.http4s.{ParseFailure, Uri}

import cron4s.CronExpr
import cron4s.circe._

import com.snowplowanalytics.snowplow.rdbloader.common.telemetry.Telemetry
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.config.{ConfigUtils, Region}
import com.snowplowanalytics.snowplow.rdbloader.config.Config._

/**
 * Main config file parsed from HOCON
 * @tparam D
 *   kind of supported warehouse
 */
case class Config[+D <: StorageTarget](
  storage: D,
  cloud: Cloud,
  jsonpaths: Option[BlobStorage.Folder],
  monitoring: Monitoring,
  retryQueue: Option[RetryQueue],
  schedules: Schedules,
  timeouts: Timeouts,
  retries: Retries,
  readyCheck: Retries,
  initRetries: Retries,
  featureFlags: FeatureFlags,
  telemetry: Telemetry.Config
)

object Config {

  val MetricsDefaultPrefix = "snowplow.rdbloader"

  def fromString[F[_]: Sync](s: String): EitherT[F, String, Config[StorageTarget]] =
    fromString(s, implicits().configDecoder)

  def fromString[F[_]: Sync](s: String, configDecoder: Decoder[Config[StorageTarget]]): EitherT[F, String, Config[StorageTarget]] = {
    implicit val implConfigDecoder: Decoder[Config[StorageTarget]] = configDecoder
    ConfigUtils.fromStringF[F, Config[StorageTarget]](s)
  }

  final case class Schedule(
    name: String,
    when: CronExpr,
    duration: FiniteDuration
  )
  final case class Schedules(
    noOperation: List[Schedule],
    optimizeEvents: Option[CronExpr] = None,
    optimizeManifest: Option[CronExpr] = None
  )
  final case class Monitoring(
    snowplow: Option[SnowplowMonitoring],
    sentry: Option[Sentry],
    metrics: Metrics,
    webhook: Option[Webhook],
    folders: Option[Folders],
    healthCheck: Option[HealthCheck]
  )
  final case class SnowplowMonitoring(appId: String, collector: String)
  final case class Sentry(dsn: URI)
  final case class HealthCheck(frequency: FiniteDuration, timeout: FiniteDuration)
  final case class Metrics(
    statsd: Option[StatsD],
    stdout: Option[Stdout],
    period: FiniteDuration
  )
  final case class StatsD(
    hostname: String,
    port: Int,
    tags: Map[String, String],
    prefix: Option[String]
  )
  final case class Stdout(prefix: Option[String])
  final case class Webhook(endpoint: Uri, tags: Map[String, String])
  final case class Folders(
    period: FiniteDuration,
    staging: BlobStorage.Folder,
    since: Option[FiniteDuration],
    transformerOutput: BlobStorage.Folder,
    until: Option[FiniteDuration],
    failBeforeAlarm: Option[Int],
    appendStagingPath: Option[Boolean]
  )
  final case class RetryQueue(
    period: FiniteDuration,
    size: Int,
    maxAttempts: Int,
    interval: FiniteDuration
  )
  final case class Timeouts(
    loading: FiniteDuration,
    nonLoading: FiniteDuration,
    sqsVisibility: FiniteDuration,
    rollbackCommit: FiniteDuration,
    rollbackConnectionValidation: FiniteDuration
  )
  final case class Retries(
    strategy: Strategy,
    attempts: Option[Int],
    backoff: FiniteDuration,
    cumulativeBound: Option[FiniteDuration]
  )
  final case class FeatureFlags(addLoadTstampColumn: Boolean)

  sealed trait Strategy
  object Strategy {
    case object Jitter extends Strategy
    case object Constant extends Strategy
    case object Exponential extends Strategy
    case object Fibonacci extends Strategy
  }

  sealed trait Cloud extends Product with Serializable

  object Cloud {

    final case class AWS(region: Region, messageQueue: AWS.SQS) extends Cloud

    object AWS {
      final case class SQS(queueName: String, region: Option[Region])
    }

    final case class GCP(messageQueue: GCP.Pubsub) extends Cloud

    object GCP {
      final case class Pubsub(
        subscription: String,
        customPubsubEndpoint: Option[String],
        parallelPullCount: Int,
        bufferSize: Int
      ) {
        val (projectId, subscriptionId) =
          subscription.split("/").toList match {
            case List("projects", project, "subscriptions", name) =>
              (project, name)
            case _ =>
              throw new IllegalArgumentException(s"Subscription format $subscription invalid")
          }
      }
    }
  }

  /**
   * All config implicits are put into case class because we want to make region decoder replaceable
   * to write unit tests for config parsing.
   */
  final case class implicits(regionConfigDecoder: Decoder[Region] = Region.regionConfigDecoder) {
    implicit val implRegionConfigDecoder: Decoder[Region] =
      regionConfigDecoder

    implicit val nullableCronExprDecoder: Decoder[Option[CronExpr]] = Decoder.instance { cur =>
      cur.as[String] match {
        case Left(other) => Left(other)
        case Right(cred) =>
          cred match {
            case "" => Right(None)
            case _ => cur.as[CronExpr].map(_.some)
          }
      }
    }

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

    implicit val timeoutsDecoder: Decoder[Timeouts] =
      deriveDecoder[Timeouts]

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

    implicit val healthCheckDecoder: Decoder[HealthCheck] =
      deriveDecoder[HealthCheck]

    implicit val monitoringDecoder: Decoder[Monitoring] =
      deriveDecoder[Monitoring]

    implicit val strategyDecoder: Decoder[Strategy] =
      Decoder[String].map(_.toUpperCase).emap {
        case "JITTER" => Strategy.Jitter.asRight
        case "CONSTANT" => Strategy.Constant.asRight
        case "EXPONENTIAL" => Strategy.Exponential.asRight
        case "FIBONACCI" => Strategy.Fibonacci.asRight
        case other => s"$other cannot be used as retry strategy. Availble choices: JITTER, CONSTANT, EXPONENTIAL, FIBONACCI".asLeft
      }

    implicit val retriesDecoder: Decoder[Retries] =
      deriveDecoder[Retries]

    implicit val retryQueueDecoder: Decoder[RetryQueue] =
      deriveDecoder[RetryQueue]

    implicit val configDecoder: Decoder[Config[StorageTarget]] =
      deriveDecoder[Config[StorageTarget]].ensure(validateConfig)

    implicit val featureFlagsConfigDecoder: Decoder[FeatureFlags] =
      deriveDecoder[FeatureFlags]

    // This decoder a bit complex since we've tried to make it backward compatible
    // after adding Pubsub as new message queue type. Also, config case classes are
    // split according to the cloud type since there is some additional cloud dependent
    // resources such as blob storage client, parameter store, region etc.
    // However, we didn't want to introduce new config field for cloud type but instead it should
    // be chosen according to message queue type. This added some additional complexity
    // to decoder.
    implicit val cloudConfigDecoder: Decoder[Cloud] =
      Decoder.instance[Cloud] { cur =>
        // We are going up one level to find out 'messageQueue' field because
        // currently we are on dummy 'cloud' field.
        val messageQueueCursor = cur.up.downField("messageQueue")
        messageQueueCursor.as[String] match {
          case Right(q) =>
            // If type of the 'messageQueue' field is string, it means that this config is for version <5.x.
            // Therefore, it should be decoded as SQS.
            cur.up.downField("region").as[Region].map(r => Cloud.AWS(r, Cloud.AWS.SQS(q, Some(r))))
          case _ =>
            messageQueueCursor.downField("type").as[String].map(_.toLowerCase) match {
              case Right("sqs") =>
                cur.up.as[Cloud.AWS]
              case Right("pubsub") =>
                cur.up.as[Cloud.GCP]
              case Right(other) =>
                Left(DecodingFailure(s"Message queue type $other is not supported yet. Supported types: 'sqs', 'pubsub'", cur.history))
              case Left(DecodingFailure(_, List(CursorOp.DownField("type")))) =>
                Left(DecodingFailure("Cannot find 'type' field in the config", cur.history))
              case Left(other) =>
                Left(other)
            }
        }
      }

    implicit val awsDecoder: Decoder[Cloud.AWS] =
      deriveDecoder[Cloud.AWS]

    implicit val sqsDecoder: Decoder[Cloud.AWS.SQS] =
      deriveDecoder[Cloud.AWS.SQS]

    implicit val gcpDecoder: Decoder[Cloud.GCP] =
      deriveDecoder[Cloud.GCP]

    implicit val pubsubDecoder: Decoder[Cloud.GCP.Pubsub] =
      deriveDecoder[Cloud.GCP.Pubsub]
  }

  /** Post-decoding validation, making sure different parts are consistent */
  def validateConfig(config: Config[StorageTarget]): List[String] =
    List(
      authMethodValidation(config),
      targetSnowflakeValidation(config),
      targetRedshiftValidation(config)
    ).flatten

  private def authMethodValidation(config: Config[StorageTarget]): List[String] =
    config.cloud match {
      case _: Config.Cloud.GCP =>
        (config.storage.foldersLoadAuthMethod, config.storage.eventsLoadAuthMethod) match {
          case (StorageTarget.LoadAuthMethod.NoCreds, StorageTarget.LoadAuthMethod.NoCreds) => Nil
          case _ => List("Only 'NoCreds' load auth method is supported with GCP")
        }
      case _ => Nil
    }

  def targetSnowflakeValidation(config: Config[StorageTarget]): List[String] =
    config.storage match {
      case storage: StorageTarget.Snowflake =>
        val monitoringError = config.monitoring.folders match {
          case None =>
            storage.folderMonitoringStage match {
              case None => None
              case Some(StorageTarget.Snowflake.Stage(name, _)) =>
                Some(s"Snowflake Loader is being provided with storage.folderMonitoringStage (${name}), but monitoring.folders is missing")
            }
          case Some(_) =>
            (storage.folderMonitoringStage, storage.loadAuthMethod) match {
              case (None, StorageTarget.LoadAuthMethod.NoCreds) =>
                Some(
                  "Snowflake Loader is configured with Folders Monitoring, but load auth method is specified as 'NoCreds' and appropriate storage.folderMonitoringStage is missing"
                )
              case _ => None
            }
        }
        val hostError = storage.host.left.toOption
        val authMethodConsistencyCheck = storage.loadAuthMethod match {
          case _: StorageTarget.LoadAuthMethod.TempCreds => None
          case StorageTarget.LoadAuthMethod.NoCreds =>
            storage.transformedStage match {
              case None => Some("'transformedStage' needs to be provided when 'NoCreds' load auth method is chosen")
              case Some(_) => None
            }
        }
        List(monitoringError, hostError, authMethodConsistencyCheck).flatten

      case _ => Nil
    }

  def targetRedshiftValidation(config: Config[StorageTarget]): List[String] =
    config.storage match {
      case storage: StorageTarget.Redshift =>
        val authMethodConsistencyCheck = storage.loadAuthMethod match {
          case _: StorageTarget.LoadAuthMethod.TempCreds => None
          case StorageTarget.LoadAuthMethod.NoCreds =>
            storage.roleArn match {
              case None => Some("roleArn needs to be provided with 'NoCreds' auth method")
              case Some(_) => None
            }
        }
        List(authMethodConsistencyCheck).flatten
      case _ => Nil
    }
}
