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
package com.snowplowanalytics.snowplow.rdbloader.common.config

import scala.concurrent.duration.Duration

import java.net.URI
import java.time.Instant

import cats.effect.Sync
import cats.data.EitherT
import cats.syntax.either._

import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._

import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common._

sealed trait ShredderConfig {
  def formats: ShredderConfig.Formats
}

object ShredderConfig {

  final case class Batch(input: URI, output: Output, queue: QueueConfig, formats: Formats, monitoring: Monitoring) extends ShredderConfig
  object Batch {
    def fromString(conf: String): Either[String, Batch] =
      fromString(conf, implicits().batchConfigDecoder)

    def fromString(conf: String, batchConfigDecoder: Decoder[Batch]): Either[String, Batch] = {
      implicit val implBatchConfigDecoder: Decoder[Batch] = batchConfigDecoder
      ConfigUtils.fromString[Batch](conf).flatMap(configCheck)
    }
  }

  final case class Stream(input: StreamInput, windowing: Duration, output: Output, queue: QueueConfig, formats: Formats) extends ShredderConfig
  object Stream {
    def fromString[F[_]: Sync](conf: String): EitherT[F, String, Stream] =
      fromString(conf, implicits().streamConfigDecoder)

    def fromString[F[_]: Sync](conf: String, streamConfigDecoder: Decoder[Stream]): EitherT[F, String, Stream] = {
      implicit val implStreamConfigDecoder: Decoder[Stream] = streamConfigDecoder
      ConfigUtils.fromStringF[F, Stream](conf).flatMap(e => EitherT.fromEither(configCheck(e)))
    }
  }

  final case class Output(path: URI, compression: Compression, region: Region)

  sealed trait Compression extends StringEnum
  object Compression {
    final case object None extends Compression { val asString = "NONE" }
    final case object Gzip extends Compression { val asString = "GZIP" }

    implicit val compressionConfigDecoder: Decoder[Compression] =
      StringEnum.decodeStringEnum[Compression]

    implicit val compressionConfigEncoder: Encoder[Compression] =
      Encoder.instance(_.toString.toUpperCase.asJson)
  }

  sealed trait StreamInput extends Product with Serializable
  object StreamInput {
    final case class Kinesis(appName: String, streamName: String, region: Region, position: InitPosition) extends StreamInput
    final case class File(dir: String) extends StreamInput
  }

  sealed trait InitPosition extends Product with Serializable
  object InitPosition {
    case object Latest extends InitPosition
    case object TrimHorizon extends InitPosition
    final case class AtTimestamp(timestamp: Instant) extends InitPosition
  }

  sealed trait QueueConfig extends Product with Serializable
  object QueueConfig {
    final case class SNS(topicArn: String, region: Region) extends QueueConfig
    final case class SQS(queueName: String, region: Region) extends QueueConfig
  }

  final case class Formats(default: LoaderMessage.Format,
                           tsv: List[SchemaCriterion],
                           json: List[SchemaCriterion],
                           skip: List[SchemaCriterion]) {
    /** Find if there are overlapping criterions in any two of known three groups */
    def findOverlaps: Set[SchemaCriterion] =
      Formats.findOverlaps(tsv, json) ++
        Formats.findOverlaps(json, skip) ++
        Formats.findOverlaps(skip, tsv)
  }

  object Formats {

    val Default: Formats = Formats(LoaderMessage.Format.TSV, Nil, Nil, Nil)

    /** Find all criterion overlaps in two lists */
    def findOverlaps(as: List[SchemaCriterion], bs: List[SchemaCriterion]): Set[SchemaCriterion] =
      as.flatMap(a => bs.map(b => (a, b))).foldLeft(Set.empty[SchemaCriterion])(aggregateMatching(overlap))

    /** Check if two criterions can have a potential overlap, i.e. a schema belongs to two groups */
    def overlap(a: SchemaCriterion, b: SchemaCriterion): Boolean = (a, b) match {
      case (SchemaCriterion(av, an, _, am, ar, aa), SchemaCriterion(bv, bn, _, bm, br, ba)) =>
        av == bv && an == bn && versionOverlap(am, bm) && versionOverlap(ar, br) && versionOverlap(aa, ba)
    }

    /** Check if two version numbers (MODEL, REVISION or ADDITION) can overlap */
    private def versionOverlap(av: Option[Int], bv: Option[Int]): Boolean = (av, bv) match {
      case (Some(aam), Some(bbm)) if aam == bbm => true // Identical and explicit - overlap
      case (Some(_), Some(_)) => false                  // Different and explicit
      case _ => true                                    // At least one is a wildcard - overlap
    }

    /** Accumulate all pairs matching predicate */
    def aggregateMatching[A](predicate: (A, A) => Boolean)(acc: Set[A], pair: (A, A)): Set[A] = (acc, pair) match {
      case (acc, (a, b)) if predicate(a, b) => acc + a + b
      case (acc, _) => acc
    }
  }

  final case class Monitoring(sentry: Option[Sentry])
  final case class Sentry(dsn: URI)

  /**
   * All config implicits are put into case class because we want to make region decoder
   * replaceable to write unit tests for config parsing.
   */
  final case class implicits(regionConfigDecoder: Decoder[Region] = Region.regionConfigDecoder) {
    implicit val implRegionConfigDecoder: Decoder[Region] =
      regionConfigDecoder

    implicit val batchConfigDecoder: Decoder[Batch] =
      deriveDecoder[Batch]

    implicit val streamConfigDecoder: Decoder[Stream] =
      deriveDecoder[Stream]

    implicit val outputConfigDecoder: Decoder[Output] =
      deriveDecoder[Output]

    implicit val streamInputConfigDecoder: Decoder[StreamInput] =
      Decoder.instance { cur =>
        val typeCur = cur.downField("type")
        typeCur.as[String].map(_.toLowerCase) match {
          case Right("file") =>
            cur.as[StreamInput.File]
          case Right("kinesis") =>
            cur.as[StreamInput.Kinesis]
          case Right(other) =>
            Left(DecodingFailure(s"Shredder input type $other is not supported yet. Supported types: 'kinesis', 's3' and 'file'", typeCur.history))
          case Left(DecodingFailure(_, List(CursorOp.DownField("type")))) =>
            Left(DecodingFailure("Cannot find 'type' string in shredder configuration", typeCur.history))
          case Left(other) =>
            Left(other)
        }
      }

    implicit val streamInputKinesisConfigDecoder: Decoder[StreamInput.Kinesis] =
      deriveDecoder[StreamInput.Kinesis]

    implicit val streamInputFileConfigDecoder: Decoder[StreamInput.File] =
      deriveDecoder[StreamInput.File]

    implicit val queueConfigDecoder: Decoder[QueueConfig] =
      Decoder.instance { cur =>
        val typeCur = cur.downField("type")
        typeCur.as[String].map(_.toLowerCase) match {
          case Right("sns") =>
            cur.as[QueueConfig.SNS]
          case Right("sqs") =>
            cur.as[QueueConfig.SQS]
          case Right(other) =>
            Left(DecodingFailure(s"Queue type $other is not supported yet. Supported types: 'SNS' and 'SQS'", typeCur.history))
          case Left(DecodingFailure(_, List(CursorOp.DownField("type")))) =>
            Left(DecodingFailure("Cannot find 'type' string in shredder configuration", typeCur.history))
          case Left(other) =>
            Left(other)
        }
      }

    implicit val snsConfigDecoder: Decoder[QueueConfig.SNS] =
      deriveDecoder[QueueConfig.SNS]

    implicit val sqsConfigDecoder: Decoder[QueueConfig.SQS] =
      deriveDecoder[QueueConfig.SQS]

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

    implicit val formatsConfigDecoder: Decoder[Formats] =
      deriveDecoder[Formats]

    implicit val monitoringConfigDecoder: Decoder[Monitoring] =
      deriveDecoder[Monitoring]

    implicit val sentryConfigDecoder: Decoder[Sentry] =
      deriveDecoder[Sentry]

    implicit val durationDecoder: Decoder[Duration] =
      Decoder[String].emap(s => Either.catchOnly[NumberFormatException](Duration(s)).leftMap(_.toString))

    implicit val uriDecoder: Decoder[URI] =
      Decoder[String].emap(s => Either.catchOnly[IllegalArgumentException](URI.create(s)).leftMap(_.toString))
  }

  def configCheck[A <: ShredderConfig](config: A): Either[String, A] = {
    val overlaps = config.formats.findOverlaps
    val message =
      s"Following schema criterions overlap in different groups (TSV, JSON, skip): " +
        s"${overlaps.map(_.asString).mkString(", ")}. " +
        s"Make sure every schema can have only one format"
    Either.cond(overlaps.isEmpty, config, message)
  }
}
