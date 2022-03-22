package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sources

import java.util.Date

import cats.syntax.either._

import cats.effect.{Sync, ContextShift, ConcurrentEffect}

import fs2.Stream

import fs2.aws.kinesis.CommittableRecord
import fs2.aws.kinesis.consumer.readFromKinesisStream
import fs2.aws.kinesis.KinesisConsumerSettings

import software.amazon.awssdk.regions.{Region => AWSRegion}
import software.amazon.kinesis.common.InitialPositionInStream

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload}

import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.InitPosition
import com.snowplowanalytics.snowplow.rdbloader.common.config.Region
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.Processing.Application

object Kinesis {

  def read[F[_]: ConcurrentEffect: ContextShift](appName: String, streamName: String, region: Region, position: InitPosition): Stream[F, ParsedF[F]] = {
    val settings = Either.catchOnly[IllegalArgumentException](AWSRegion.of(region.name)) match {
      case Right(region) =>
        Sync[F].pure(KinesisConsumerSettings(streamName, appName, region, initialPositionInStream = fromConfig(position)))
      case Left(error) =>
        Sync[F].raiseError[KinesisConsumerSettings](new IllegalArgumentException(s"Cannot parse ${region.name} as valid AWS region", error))
    }

    for {
      settings <- Stream.eval(settings)
      record   <- readFromKinesisStream(settings)
    } yield (parse(record), record.checkpoint)
  }


  def parse(record: CommittableRecord): Parsed = {
    val buf = new Array[Byte](record.record.data().remaining())
    record.record.data().get(buf)
    val str = new String(buf, "UTF-8")
    Event.parse(str).toEither.leftMap { error =>
      BadRow.LoaderParsingError(Application, error, Payload.RawPayload(str))
    }
  }


  /** Turn it into fs2-aws-compatible structure */
  def fromConfig(initPosition: InitPosition): Either[InitialPositionInStream, Date] =
    initPosition match {
      case InitPosition.Latest            => InitialPositionInStream.LATEST.asLeft
      case InitPosition.TrimHorizon       => InitialPositionInStream.TRIM_HORIZON.asLeft
      case InitPosition.AtTimestamp(date) => Date.from(date).asRight
    }
}
