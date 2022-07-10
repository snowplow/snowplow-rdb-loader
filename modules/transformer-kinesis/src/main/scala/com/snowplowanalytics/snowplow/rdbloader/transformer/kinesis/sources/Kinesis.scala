package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sources

import java.util.{Date, UUID}
import java.net.{InetAddress, URI}

import cats.Applicative
import cats.implicits._

import cats.effect._

import eu.timepit.refined._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric._

import fs2.Stream

import fs2.aws.kinesis.{CommittableRecord, Kinesis => Fs2Kinesis, KinesisConsumerSettings}

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

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload}

import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.Processing.Application
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.Config.{InitPosition, Retrieval}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.Config.StreamInput.{Kinesis => KinesisConfig}

object Kinesis {

  def read[F[_]: ConcurrentEffect: ContextShift: Timer](blocker: Blocker, config: KinesisConfig, cloudwatchEnabled: Boolean): Stream[F, ParsedC[KinesisCheckpointer[F]]] =
    for {
      region <- Stream.emit(AWSRegion.of(config.region.name))
      bufferSize <- Stream.eval[F, Int Refined Positive](
        refineV[Positive](config.bufferSize) match {
          case Right(mc) => Sync[F].pure(mc)
          case Left(e) =>
            Sync[F].raiseError(
              new IllegalArgumentException(s"${config.bufferSize} can't be refined as positive: $e")
            )
        }
      )
      consumerSettings = KinesisConsumerSettings(config.streamName, config.appName, bufferSize = bufferSize)
      kinesisClient <- Stream.resource(mkKinesisClient[F](region, config.customEndpoint))
      dynamoClient <- Stream.resource(mkDynamoDbClient[F](region, config.dynamodbCustomEndpoint))
      cloudWatchClient <- Stream.resource(mkCloudWatchClient[F](region, config.cloudwatchCustomEndpoint))
      kinesis = Fs2Kinesis.create(blocker, scheduler(kinesisClient, dynamoClient, cloudWatchClient, config, cloudwatchEnabled, _))
      record  <- kinesis.readFromKinesisStream(consumerSettings)
    } yield (parse(record), checkpointer(record))

  def parse(record: CommittableRecord): Parsed = {
    val buf = new Array[Byte](record.record.data().remaining())
    record.record.data().get(buf)
    val str = new String(buf, "UTF-8")
    Event.parse(str).toEither.leftMap { error =>
      BadRow.LoaderParsingError(Application, error, Payload.RawPayload(str))
    }
  }

  def checkpointer[F[_]: Sync](record: CommittableRecord): KinesisCheckpointer[F] =
    KinesisCheckpointer[F](Map(record.shardId -> record.checkpoint))


  case class KinesisCheckpointer[F[_]](byShard: Map[String, F[Unit]])

  object KinesisCheckpointer {
    implicit def kinesisCheckpointer[F[_]: Applicative]: Checkpointer[F, KinesisCheckpointer[F]] = new Checkpointer[F, KinesisCheckpointer[F]] {
      def checkpoint(c: KinesisCheckpointer[F]): F[Unit] = c.byShard.values.toList.sequence_
      def combine(older: KinesisCheckpointer[F], newer: KinesisCheckpointer[F]): KinesisCheckpointer[F] =
        KinesisCheckpointer[F](byShard = older.byShard ++ newer.byShard) // order is important!
      def empty: KinesisCheckpointer[F] =
        KinesisCheckpointer(Map.empty)
    }

  }

  private def scheduler[F[_]: Sync](
                                     kinesisClient: KinesisAsyncClient,
                                     dynamoDbClient: DynamoDbAsyncClient,
                                     cloudWatchClient: CloudWatchAsyncClient,
                                     kinesisConfig: KinesisConfig,
                                     cloudWatchEnabled: Boolean,
                                     recordProcessorFactory: ShardRecordProcessorFactory
                                   ): F[Scheduler] = {
    def build(uuid: UUID, hostname: String): F[Scheduler] = {

      val configsBuilder =
        new ConfigsBuilder(kinesisConfig.streamName,
          kinesisConfig.appName,
          kinesisClient,
          dynamoDbClient,
          cloudWatchClient,
          s"$hostname:$uuid",
          recordProcessorFactory
        )

      val initPositionExtended = kinesisConfig.position match {
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
            kinesisConfig.retrievalMode match {
              case Retrieval.FanOut =>
                new FanOutConfig(kinesisClient).streamName(kinesisConfig.streamName).applicationName(kinesisConfig.appName)
              case Retrieval.Polling(maxRecords) =>
                new PollingConfig(kinesisConfig.streamName, kinesisClient).maxRecords(maxRecords)
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
      uuid      <- Sync[F].delay(UUID.randomUUID)
      hostname  <- Sync[F].delay(InetAddress.getLocalHost().getCanonicalHostName)
      scheduler <- build(uuid, hostname)
    } yield scheduler
  }

  private def mkKinesisClient[F[_]: Sync](region: AWSRegion, customEndpoint: Option[URI]): Resource[F, KinesisAsyncClient] =
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

  private def mkDynamoDbClient[F[_]: Sync](region: AWSRegion, customEndpoint: Option[URI]): Resource[F, DynamoDbAsyncClient] =
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

  private def mkCloudWatchClient[F[_]: Sync](region: AWSRegion, customEndpoint: Option[URI]): Resource[F, CloudWatchAsyncClient] =
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
}
