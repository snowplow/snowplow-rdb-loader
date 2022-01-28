package com.snowplowanalytics.snowplow.rdbloader.shredder.stream

import java.net.URI

import cats.implicits._

import cats.effect.{Sync, Clock}

import io.circe.syntax.EncoderOps

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data

import com.snowplowanalytics.snowplow.rdbloader.common.{S3, LoaderMessage}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{Timestamps, Format, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks.Window

import com.snowplowanalytics.aws.AWSQueue

object Completion {

  private final val MessageProcessorVersion = Semver
    .decodeSemver(BuildInfo.version)
    .fold(e => throw new IllegalStateException(s"Cannot parse project version $e"), identity)
  final val MessageProcessor: LoaderMessage.Processor =
    LoaderMessage.Processor(BuildInfo.name, MessageProcessorVersion)

  final val MessageGroupId = "shredding"

  /**
   * Finalize the batch by sending a `ShreddingComplete` SQS message
   *
   * @param compression a compression type used in the batch
   * @param isTabular a predicate to derive type of output for a schema key
   * @param root S3 batch root (with output=good and output=bad)
   * @param awsQueue AWSQueue instance to send the message to
   * @param window run id (when batch has been started)
   * @param state all metadata shredder extracted from a batch
   */
  def seal[F[_]: Clock: Sync](compression: Compression,
                              isTabular: SchemaKey => Boolean,
                              root: URI,
                              awsQueue: AWSQueue[F])
                             (window: Window, state: State): F[Unit] = {
    val shreddedTypes: List[ShreddedType] =
      state.types.toList.map {
        shreddedType => {
          val schemaKey = shreddedType.schemaKey
          val shredProperty = shreddedType.shredProperty match {
            case _: Data.Contexts => ShreddedType.Contexts
            case Data.UnstructEvent => ShreddedType.SelfDescribingEvent
          }
          val format = if (isTabular(schemaKey)) Format.TSV else Format.JSON
          ShreddedType(schemaKey, format, shredProperty)
        }
      }
    for {
      timestamps <- Clock[F].instantNow.map { now =>
        Timestamps(window.toInstant, now, state.minCollector, state.maxCollector)
      }
      base = getBasePath(S3.Folder.coerce(root.toString), window)
      count = LoaderMessage.Count(state.total - state.bad)
      message = LoaderMessage.ShreddingComplete(base, shreddedTypes, timestamps, compression, MessageProcessor, Some(count))
      body = message.selfDescribingData.asJson.noSpaces
      _ <- awsQueue.sendMessage(Some(MessageGroupId), body)
    } yield ()
  }

  def getBasePath(bucket: String, prefix: String, window: Window): S3.Folder =
    S3.Folder.coerce(s"s3://$bucket/$prefix${window.getDir}")

  def getBasePath(folder: S3.Folder, window: Window): S3.Folder = {
    val (bucket, prefix) = S3.splitS3Path(folder)
    getBasePath(bucket, prefix, window)
  }
}
