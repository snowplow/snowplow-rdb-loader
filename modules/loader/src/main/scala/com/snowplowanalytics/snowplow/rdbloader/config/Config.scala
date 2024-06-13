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
package com.snowplowanalytics.snowplow.rdbloader.config

import java.net.URI
import scala.concurrent.duration.{Duration, FiniteDuration}
import cats.effect.Sync
import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.option._
import com.snowplowanalytics.iglu.core.SchemaCriterion
import io.circe._
import io.circe.generic.semiauto._
import org.http4s.{ParseFailure, Uri}
import cron4s.CronExpr
import cron4s.circe._
import com.snowplowanalytics.snowplow.rdbloader.common.telemetry.Telemetry
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.config.args.HoconOrPath
import com.snowplowanalytics.snowplow.rdbloader.common.config.{ConfigUtils, License, Region}
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
  telemetry: Telemetry.Config,
  license: License
)

object Config {

  val MetricsDefaultPrefix = "snowplow.rdbloader"

  def parseAppConfig[F[_]: Sync](config: HoconOrPath): EitherT[F, String, Config[StorageTarget]] =
    parseAppConfig(config, implicits().configDecoder)

  def parseAppConfig[F[_]: Sync](
    config: HoconOrPath,
    configDecoder: Decoder[Config[StorageTarget]]
  ): EitherT[F, String, Config[StorageTarget]] = {
    implicit val implConfigDecoder: Decoder[Config[StorageTarget]] = configDecoder
    ConfigUtils.parseAppConfigF[F, Config[StorageTarget]](config)
  }

  final case class Schedule(
    name: String,
    when: CronExpr,
    duration: FiniteDuration
  )
  final case class Schedules(
    noOperation: List[Schedule],
    optimizeEvents: Option[CronExpr]   = None,
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
    connectionIsValid: FiniteDuration
  ) {
    def totalTimeToRollBack: FiniteDuration = rollbackCommit + connectionIsValid
  }
  final case class Retries(
    strategy: Strategy,
    attempts: Option[Int],
    backoff: FiniteDuration,
    cumulativeBound: Option[FiniteDuration]
  )
  final case class FeatureFlags(addLoadTstampColumn: Boolean, disableRecovery: List[SchemaCriterion])

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
        awaitTerminatePeriod: FiniteDuration,
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

    final case class Azure(
      blobStorageEndpoint: URI,
      messageQueue: Azure.Kafka,
      azureVaultName: Option[String]
    ) extends Cloud

    object Azure {
      final case class Kafka(
        topicName: String,
        bootstrapServers: String,
        consumerConf: Map[String, String]
      )
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
            case _  => cur.as[CronExpr].map(_.some)
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
        case "JITTER"      => Strategy.Jitter.asRight
        case "CONSTANT"    => Strategy.Constant.asRight
        case "EXPONENTIAL" => Strategy.Exponential.asRight
        case "FIBONACCI"   => Strategy.Fibonacci.asRight
        case other         => s"$other cannot be used as retry strategy. Availble choices: JITTER, CONSTANT, EXPONENTIAL, FIBONACCI".asLeft
      }

    implicit val retriesDecoder: Decoder[Retries] =
      deriveDecoder[Retries]

    implicit val retryQueueDecoder: Decoder[RetryQueue] =
      deriveDecoder[RetryQueue]

    implicit val configDecoder: Decoder[Config[StorageTarget]] =
      deriveDecoder[Config[StorageTarget]].ensure(validateConfig)

    implicit val schemaCriterionConfigDecoder: Decoder[SchemaCriterion] =
      Decoder[String].emap(s => SchemaCriterion.parse(s).toRight(s"[$s] is not a valid schema criterion"))

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
              case Right("kafka") =>
                cur.up.as[Cloud.Azure]
              case Right(other) =>
                Left(
                  DecodingFailure(s"Message queue type $other is not supported yet. Supported types: 'sqs', 'pubsub', 'kafka'", cur.history)
                )
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

    implicit val azureDecoder: Decoder[Cloud.Azure] =
      deriveDecoder[Cloud.Azure]

    implicit val pubsubDecoder: Decoder[Cloud.GCP.Pubsub] =
      deriveDecoder[Cloud.GCP.Pubsub]

    implicit val kafkaDecoder: Decoder[Cloud.Azure.Kafka] =
      deriveDecoder[Cloud.Azure.Kafka]
  }

  /** Post-decoding validation, making sure different parts are consistent */
  def validateConfig(config: Config[StorageTarget]): List[String] =
    List(
      authMethodValidation(config.storage.eventsLoadAuthMethod, config.cloud),
      authMethodValidation(config.storage.foldersLoadAuthMethod, config.cloud),
      azureVaultCheck(config),
      targetSnowflakeValidation(config),
      targetRedshiftValidation(config)
    ).flatten

  private def azureVaultCheck(config: Config[StorageTarget]): List[String] =
    config.cloud match {
      case c: Config.Cloud.Azure if c.azureVaultName.isEmpty =>
        (config.storage.password, config.storage.sshTunnel.flatMap(_.bastion.key)) match {
          case (_: StorageTarget.PasswordConfig.EncryptedKey, _) | (_, Some(_)) => List("Azure vault name is needed")
          case _                                                                => Nil
        }
      case _ => Nil
    }

  private def authMethodValidation(loadAuthMethod: StorageTarget.LoadAuthMethod, cloud: Config.Cloud): List[String] =
    cloud match {
      case _: Config.Cloud.GCP =>
        loadAuthMethod match {
          case StorageTarget.LoadAuthMethod.NoCreds => Nil
          case _                                    => List("Only 'NoCreds' load auth method is supported with GCP")
        }
      case _: Config.Cloud.AWS =>
        loadAuthMethod match {
          case StorageTarget.LoadAuthMethod.NoCreds                   => Nil
          case _: StorageTarget.LoadAuthMethod.TempCreds.AWSTempCreds => Nil
          case _                                                      => List("Given 'TempCreds' configuration isn't suitable for AWS")
        }
      case _: Config.Cloud.Azure =>
        loadAuthMethod match {
          case StorageTarget.LoadAuthMethod.NoCreds                     => Nil
          case _: StorageTarget.LoadAuthMethod.TempCreds.AzureTempCreds => Nil
          case _                                                        => List("Given 'TempCreds' configuration isn't suitable for Azure")
        }
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
              case None    => Some("'transformedStage' needs to be provided when 'NoCreds' load auth method is chosen")
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
              case None    => Some("roleArn needs to be provided with 'NoCreds' auth method")
              case Some(_) => None
            }
        }
        List(authMethodConsistencyCheck).flatten
      case _ => Nil
    }
}
