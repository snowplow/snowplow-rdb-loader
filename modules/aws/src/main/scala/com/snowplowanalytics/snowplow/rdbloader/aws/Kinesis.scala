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
package com.snowplowanalytics.snowplow.rdbloader.aws

import java.util.{Date, UUID}
import java.net.{InetAddress, URI}

import cats.Applicative
import cats.implicits._
import cats.effect._

import eu.timepit.refined._
import eu.timepit.refined.api.Refined
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

import com.snowplowanalytics.snowplow.rdbloader.common.config.Kinesis._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue.Consumer
import com.snowplowanalytics.snowplow.rdbloader.common.config.Region

object Kinesis {

  case class Message[F[_]: Sync](record: CommittableRecord) extends Queue.Consumer.Message[F] {
    override def content: String = getContent(record)
    override def ack: F[Unit] = record.checkpoint
  }

  def consumer[F[_]: Async: Applicative](
    appName: String,
    streamName: String,
    region: Region,
    position: InitPosition,
    retrievalMode: Retrieval,
    bufferSize: Int,
    customEndpoint: Option[URI],
    dynamodbCustomEndpoint: Option[URI],
    cloudwatchCustomEndpoint: Option[URI],
    cloudwatchEnabled: Boolean
  ): Resource[F, Queue.Consumer[F]] =
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
                     kinesis
                       .readFromKinesisStream(consumerSettings)
                       .map(r => Message(r))
                 }
    } yield consumer

  def getContent(record: CommittableRecord): String = {
    val buf = new Array[Byte](record.record.data().remaining())
    record.record.data().get(buf)
    new String(buf, "UTF-8")
  }

  private def scheduler[F[_]: Sync](
    kinesisClient: KinesisAsyncClient,
    dynamoDbClient: DynamoDbAsyncClient,
    cloudWatchClient: CloudWatchAsyncClient,
    streamName: String,
    appName: String,
    position: InitPosition,
    retrievalMode: Retrieval,
    cloudWatchEnabled: Boolean
  )(
    recordProcessorFactory: ShardRecordProcessorFactory
  ): F[Scheduler] = {
    def build(uuid: UUID, hostname: String): F[Scheduler] = {

      val configsBuilder =
        new ConfigsBuilder(streamName, appName, kinesisClient, dynamoDbClient, cloudWatchClient, s"$hostname:$uuid", recordProcessorFactory)

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
