/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.common.cloud.aws

import java.util.{Date, UUID}
import java.net.{InetAddress, URI}
import java.time.Instant
import cats.Applicative
import io.circe._
import io.circe.generic.semiauto._
import cats.implicits._
import cats.effect._
import eu.timepit.refined._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric._
import fs2.Stream
import fs2.aws.kinesis.{CommittableRecord, KinesisConsumerSettings, Kinesis => Fs2Kinesis}
import software.amazon.awssdk.regions.{Region => AWSRegion}
import software.amazon.kinesis.common.{ConfigsBuilder, InitialPositionInStream, InitialPositionInStreamExtended}
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.metrics.MetricsLevel
import software.amazon.kinesis.processor.ShardRecordProcessorFactory
import software.amazon.kinesis.retrieval.polling.PollingConfig
import software.amazon.kinesis.retrieval.fanout.FanOutConfig
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue.Consumer
import com.snowplowanalytics.snowplow.rdbloader.common.config.Region

object Kinesis {

  case class Message[F[_]](content: String, ack: F[Unit], shardId: String) extends Queue.Consumer.Message[F]

  def consumer[F[_] : ConcurrentEffect : ContextShift : Timer : Applicative](blocker: Blocker,
                                                                             appName: String,
                                                                             streamName: String,
                                                                             region: Region,
                                                                             position: InitPosition,
                                                                             retrievalMode: Retrieval,
                                                                             bufferSize: Int,
                                                                             customEndpoint: Option[URI],
                                                                             dynamodbCustomEndpoint: Option[URI],
                                                                             cloudwatchCustomEndpoint: Option[URI],
                                                                             cloudwatchEnabled: Boolean): Resource[F, Queue.Consumer[F]] =
    for {
      region <- Resource.pure[F, AWSRegion](AWSRegion.of(region.name))
      bufferSize <- Resource.eval[F, Int Refined Positive](
        refineV[Positive](bufferSize) match {
          case Right(mc) => Sync[F].pure(mc)
          case Left(e) =>
            Sync[F].raiseError(
              new IllegalArgumentException(s"$bufferSize can't be refined as positive: $e")
            )
        }
      )
      consumerSettings = KinesisConsumerSettings(streamName, appName, bufferSize = bufferSize)
      kinesisClient <- mkKinesisClient[F](region, customEndpoint)
      dynamoClient <- mkDynamoDbClient[F](region, dynamodbCustomEndpoint)
      cloudWatchClient <- mkCloudWatchClient[F](region, cloudwatchCustomEndpoint)
      kinesis = Fs2Kinesis.create(
        blocker,
        scheduler[F](
          kinesisClient,
          dynamoClient,
          cloudWatchClient,
          streamName,
          appName,
          position,
          retrievalMode,
          cloudwatchEnabled
        )
      )
      consumer = new Queue.Consumer[F] {
        override def read: Stream[F, Consumer.Message[F]] =
          kinesis.readFromKinesisStream(consumerSettings)
            .map(r => Message(getContent(r), r.checkpoint, r.shardId))
      }
    } yield consumer

  def getContent(record: CommittableRecord): String = {
    val buf = new Array[Byte](record.record.data().remaining())
    record.record.data().get(buf)
    new String(buf, "UTF-8")
  }

  private def scheduler[F[_] : Sync](kinesisClient: KinesisAsyncClient,
                                     dynamoDbClient: DynamoDbAsyncClient,
                                     cloudWatchClient: CloudWatchAsyncClient,
                                     streamName: String,
                                     appName: String,
                                     position: InitPosition,
                                     retrievalMode: Retrieval,
                                     cloudWatchEnabled: Boolean)
                                    (recordProcessorFactory: ShardRecordProcessorFactory): F[Scheduler] = {
    def build(uuid: UUID, hostname: String): F[Scheduler] = {

      val configsBuilder =
        new ConfigsBuilder(streamName,
          appName,
          kinesisClient,
          dynamoDbClient,
          cloudWatchClient,
          s"$hostname:$uuid",
          recordProcessorFactory
        )

      val initPositionExtended = position match {
        case InitPosition.Latest =>
          InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)
        case InitPosition.TrimHorizon =>
          InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
        case InitPosition.AtTimestamp(timestamp) =>
          InitialPositionInStreamExtended.newInitialPositionAtTimestamp(Date.from(timestamp))
      }

      val retrievalConfig =
        configsBuilder.retrievalConfig
          .initialPositionInStreamExtended(initPositionExtended)
          .retrievalSpecificConfig {
            retrievalMode match {
              case Retrieval.FanOut =>
                new FanOutConfig(kinesisClient).streamName(streamName).applicationName(appName)
              case Retrieval.Polling(maxRecords) =>
                new PollingConfig(streamName, kinesisClient).maxRecords(maxRecords)
            }
          }

      val metricsConfig = configsBuilder.metricsConfig.metricsLevel {
        if (cloudWatchEnabled) MetricsLevel.DETAILED else MetricsLevel.NONE
      }

      Sync[F].delay {
        new Scheduler(
          configsBuilder.checkpointConfig,
          configsBuilder.coordinatorConfig,
          configsBuilder.leaseManagementConfig,
          configsBuilder.lifecycleConfig,
          metricsConfig,
          configsBuilder.processorConfig,
          retrievalConfig
        )
      }
    }

    for {
      uuid <- Sync[F].delay(UUID.randomUUID)
      hostname <- Sync[F].delay(InetAddress.getLocalHost().getCanonicalHostName)
      scheduler <- build(uuid, hostname)
    } yield scheduler
  }

  private def mkKinesisClient[F[_] : Sync](region: AWSRegion, customEndpoint: Option[URI]): Resource[F, KinesisAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        val builder =
          KinesisAsyncClient
            .builder()
            .region(region)
        val customized = customEndpoint.map(builder.endpointOverride).getOrElse(builder)
        customized.build
      }
    }

  private def mkDynamoDbClient[F[_] : Sync](region: AWSRegion, customEndpoint: Option[URI]): Resource[F, DynamoDbAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        val builder =
          DynamoDbAsyncClient
            .builder()
            .region(region)
        val customized = customEndpoint.map(builder.endpointOverride).getOrElse(builder)
        customized.build
      }
    }

  private def mkCloudWatchClient[F[_] : Sync](region: AWSRegion, customEndpoint: Option[URI]): Resource[F, CloudWatchAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        val builder =
          CloudWatchAsyncClient
            .builder()
            .region(region)
        val customized = customEndpoint.map(builder.endpointOverride).getOrElse(builder)
        customized.build
      }
    }

  sealed trait InitPosition extends Product with Serializable

  object InitPosition {
    case object Latest extends InitPosition

    case object TrimHorizon extends InitPosition

    final case class AtTimestamp(timestamp: Instant) extends InitPosition

    implicit val initPositionConfigDecoder: Decoder[InitPosition] =
      Decoder.decodeJson.emap { json =>
        json.asString match {
          case Some("TRIM_HORIZON") => InitPosition.TrimHorizon.asRight
          case Some("LATEST") => InitPosition.Latest.asRight
          case Some(other) =>
            s"Initial position $other is unknown. Choose from LATEST and TRIM_HORIZON. AT_TIMESTAMP must provide the timestamp".asLeft
          case None =>
            val result = for {
              root <- json.asObject.map(_.toMap)
              atTimestamp <- root.get("AT_TIMESTAMP")
              atTimestampObj <- atTimestamp.asObject.map(_.toMap)
              timestampStr <- atTimestampObj.get("timestamp")
              timestamp <- timestampStr.as[Instant].toOption
            } yield InitPosition.AtTimestamp(timestamp)
            result match {
              case Some(atTimestamp) => atTimestamp.asRight
              case None =>
                "Initial position can be either LATEST or TRIM_HORIZON string or AT_TIMESTAMP object (e.g. 2020-06-03T00:00:00Z)".asLeft
            }
        }
      }
  }

  sealed trait Retrieval

  object Retrieval {
    case class Polling(maxRecords: Int) extends Retrieval

    case object FanOut extends Retrieval

    case class RetrievalRaw(`type`: String, maxRecords: Option[Int])

    implicit val retrievalRawDecoder: Decoder[RetrievalRaw] = deriveDecoder[RetrievalRaw]

    implicit val retrievalDecoder: Decoder[Retrieval] =
      Decoder.instance { cur =>
        for {
          rawParsed <- cur.as[RetrievalRaw].map(raw => raw.copy(`type` = raw.`type`.toUpperCase))
          retrieval <- rawParsed match {
            case RetrievalRaw("POLLING", Some(maxRecords)) =>
              Polling(maxRecords).asRight
            case RetrievalRaw("FANOUT", _) =>
              FanOut.asRight
            case other =>
              DecodingFailure(
                s"Retrieval mode $other is not supported. Possible types are FanOut and Polling (must provide maxRecords field)",
                cur.history
              ).asLeft
          }
        } yield retrieval
      }
    implicit val retrievalEncoder: Encoder[Retrieval] = deriveEncoder[Retrieval]
  }
}
