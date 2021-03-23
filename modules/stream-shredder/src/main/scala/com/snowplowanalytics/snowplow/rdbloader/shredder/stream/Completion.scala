package com.snowplowanalytics.snowplow.rdbloader.shredder.stream

import java.net.URI

import cats.implicits._

import cats.effect.{Sync, Clock}

import io.circe.syntax.EncoderOps

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.rdbloader.common.{S3, LoaderMessage}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{Timestamps, Format, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.common.config.Config.Shredder.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks.Window

import com.snowplowanalytics.aws.sqs.SQS

object Completion {

  private final val MessageProcessorVersion = Semver
    .decodeSemver(BuildInfo.version)
    .fold(e => throw new IllegalStateException(s"Cannot parse project version $e"), identity)
  final val MessageProcessor: LoaderMessage.Processor =
    LoaderMessage.Processor(BuildInfo.name, MessageProcessorVersion)

  final val MessageGroupId = "shredding"

  def seal[F[_]: Clock: Sync](compression: Compression, isTabular: SchemaKey => Boolean, root: URI, queueName: String)
                             (window: Window, state: State): F[Unit] = {
    val shreddedTypes: List[ShreddedType] = state.types.toList.map { key =>
      if (isTabular(key)) ShreddedType(key, Format.TSV) else ShreddedType(key, Format.JSON)
    }
    for {
      timestamps <- Clock[F].instantNow.map { now =>
        Timestamps(window.toInstant, now, state.minCollector, state.maxCollector)
      }
      base = getBasePath(S3.Folder.coerce(root.toString), window)
      message = LoaderMessage.ShreddingComplete(base, shreddedTypes, timestamps, compression, MessageProcessor)
      body = message.selfDescribingData.asJson.noSpaces
      _ <- SQS.mkClient[F].use(client => SQS.sendMessage[F](client)(queueName, Some(MessageGroupId), body))
    } yield ()
  }

  def getBasePath(bucket: String, prefix: String, window: Window): S3.Folder =
    S3.Folder.coerce(s"s3://$bucket/$prefix${window.getDir}")

  def getBasePath(folder: S3.Folder, window: Window): S3.Folder = {
    val (bucket, prefix) = S3.splitS3Path(folder)
    getBasePath(bucket, prefix, window)
  }
}
