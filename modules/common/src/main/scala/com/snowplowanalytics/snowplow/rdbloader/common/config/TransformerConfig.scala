/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.common.config

import java.net.URI
import java.time.Instant

import cats.syntax.either._

import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._

import com.snowplowanalytics.iglu.core.SchemaCriterion

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.config.implicits._
import com.snowplowanalytics.snowplow.rdbloader.common._

object TransformerConfig {

  sealed trait Compression extends StringEnum
  object Compression {
    final case object None extends Compression { val asString = "NONE" }
    final case object Gzip extends Compression { val asString = "GZIP" }

    implicit val compressionConfigDecoder: Decoder[Compression] =
      StringEnum.decodeStringEnum[Compression]

    implicit val compressionConfigEncoder: Encoder[Compression] =
      Encoder.instance(_.toString.toUpperCase.asJson)
  }

  sealed trait Formats extends Product with Serializable

  object Formats {
    sealed trait WideRow extends Formats

    object WideRow {
      final case object JSON extends WideRow
      final case object PARQUET extends WideRow
    }

    final case class Shred(
      default: LoaderMessage.TypesInfo.Shredded.ShreddedFormat,
      tsv: List[SchemaCriterion],
      json: List[SchemaCriterion],
      skip: List[SchemaCriterion]
    ) extends Formats {

      /** Find if there are overlapping criterions in any two of known three groups */
      def findOverlaps: Set[SchemaCriterion] =
        Shred.findOverlaps(tsv, json) ++
          Shred.findOverlaps(json, skip) ++
          Shred.findOverlaps(skip, tsv)
    }

    object Shred {

      val Default: Formats = Shred(LoaderMessage.TypesInfo.Shredded.ShreddedFormat.TSV, Nil, Nil, Nil)

      /** Find all criterion overlaps in two lists */
      def findOverlaps(as: List[SchemaCriterion], bs: List[SchemaCriterion]): Set[SchemaCriterion] =
        as.flatMap(a => bs.map(b => (a, b))).foldLeft(Set.empty[SchemaCriterion])(aggregateMatching(overlap))

      /**
       * Check if two criterions can have a potential overlap, i.e. a schema belongs to two groups
       */
      def overlap(a: SchemaCriterion, b: SchemaCriterion): Boolean = (a, b) match {
        case (SchemaCriterion(av, an, _, am, ar, aa), SchemaCriterion(bv, bn, _, bm, br, ba)) =>
          av == bv && an == bn && versionOverlap(am, bm) && versionOverlap(ar, br) && versionOverlap(aa, ba)
      }

      /** Check if two version numbers (MODEL, REVISION or ADDITION) can overlap */
      private def versionOverlap(av: Option[Int], bv: Option[Int]): Boolean = (av, bv) match {
        case (Some(aam), Some(bbm)) if aam == bbm => true // Identical and explicit - overlap
        case (Some(_), Some(_)) => false // Different and explicit
        case _ => true // At least one is a wildcard - overlap
      }

      /** Accumulate all pairs matching predicate */
      def aggregateMatching[A](predicate: (A, A) => Boolean)(acc: Set[A], pair: (A, A)): Set[A] = (acc, pair) match {
        case (acc, (a, b)) if predicate(a, b) => acc + a + b
        case (acc, _) => acc
      }
    }

    implicit val formatsConfigDecoder: Decoder[Formats] =
      Decoder.instance { cur =>
        val typeCur = cur.downField("transformationType")
        typeCur.as[String].map(_.toLowerCase) match {
          case Right("shred") =>
            cur.as[Formats.Shred]
          case Right("widerow") =>
            cur.as[Formats.WideRow]
          case Right(other) =>
            Left(DecodingFailure(s"Transformation type $other is not supported yet. Supported types: 'shred', 'widerow'", typeCur.history))
          case Left(DecodingFailure(_, List(CursorOp.DownField("type")))) =>
            Left(DecodingFailure("Cannot find 'type' string in format configuration", typeCur.history))
          case Left(other) =>
            Left(other)
        }
      }

    implicit val shredFormatsConfigDecoder: Decoder[Formats.Shred] =
      deriveDecoder[Formats.Shred]

    implicit val wideRowFormatsConfigDecoder: Decoder[Formats.WideRow] =
      Decoder.instance { cur =>
        val fileFormatCur = cur.downField("fileFormat")
        fileFormatCur.as[String].map(_.toLowerCase) match {
          case Right("json") =>
            Right(Formats.WideRow.JSON)
          case Right("parquet") =>
            Right(Formats.WideRow.PARQUET)
          case Right(other) =>
            Left(DecodingFailure(s"Widerow file format type $other is not supported yet. Supported types: 'json'", fileFormatCur.history))
          case Left(DecodingFailure(_, List(CursorOp.DownField("fileFormat")))) =>
            Left(DecodingFailure("Cannot find 'fileFormat' string in format configuration", fileFormatCur.history))
          case Left(other) =>
            Left(other)
        }
      }
  }

  final case class Validations(minimumTimestamp: Option[Instant])
  object Validations {
    implicit val validationsDecoder: Decoder[Validations] =
      deriveDecoder[Validations]
  }

  final case class Sentry(dsn: URI)
  object Sentry {
    implicit val sentryConfigDecoder: Decoder[Sentry] =
      deriveDecoder[Sentry]
  }

  final case class FeatureFlags(
    legacyMessageFormat: Boolean,
    sparkCacheEnabled: Option[Boolean],
    enableMaxRecordsPerFile: Boolean,
    truncateAtomicFields: Boolean
  )

  object FeatureFlags {
    implicit val featureFlagsConfigDecoder: Decoder[FeatureFlags] =
      deriveDecoder[FeatureFlags]
  }

  trait RegionDecodable {
    implicit def regionDecoder: Decoder[Region]
  }

  trait ImpureRegionDecodable extends RegionDecodable {
    override implicit val regionDecoder: Decoder[Region] =
      Region.regionConfigDecoder
  }

  /**
   * All config implicits are put into trait because we want to make region decoder replaceable to
   * write unit tests for config parsing.
   */
  trait Decoders extends RegionDecodable

  def formatsCheck(formats: Formats): Either[String, Formats] =
    formats match {
      case _: Formats.WideRow => formats.asRight
      case s: Formats.Shred =>
        val overlaps = s.findOverlaps
        val message =
          s"Following schema criterions overlap in different groups (TSV, JSON, skip): " +
            s"${overlaps.map(_.asString).mkString(", ")}. " +
            s"Make sure every schema can have only one format"
        Either.cond(overlaps.isEmpty, formats, message)
    }
}
