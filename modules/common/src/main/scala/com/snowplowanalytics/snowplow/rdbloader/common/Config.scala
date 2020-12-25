/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.common

import java.net.URI
import java.util.UUID

import cats.syntax.either._
import cats.syntax.show._

import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._

import com.snowplowanalytics.iglu.core.SchemaCriterion

import com.typesafe.config.ConfigFactory

import pureconfig._
import pureconfig.module.circe._
import pureconfig.error.{CannotParse, ConfigReaderFailures}

import monocle.Lens
import monocle.macros.GenLens

import Config._

/**
 * Main config file parsed from HOCON
 * @tparam D kind of supported warehouse
 */
final case class Config[+D <: StorageTarget](name: String,
                                             id: UUID,
                                             region: String,
                                             jsonpaths: Option[S3.Folder],
                                             monitoring: Monitoring,
                                             messageQueue: String,
                                             shredder: Shredder,
                                             storage: D,
                                             formats: Formats,
                                             steps: Set[Step])

object Config {

  def fromString(s: String): Either[String, Config[StorageTarget]] =
    Either
      .catchNonFatal(ConfigSource.fromConfig(ConfigFactory.parseString(s)))
      .leftMap(error => ConfigReaderFailures(CannotParse(s"Not valid HOCON. ${error.getMessage}", None)))
      .flatMap { config =>
        config
          .load[Json]
          .flatMap { json =>
            json.as[Config[StorageTarget]].leftMap(failure => ConfigReaderFailures(CannotParse(failure.show, None)))
          }
      }
      .leftMap(_.prettyPrint())
      .flatMap { config =>
        val overlaps = config.formats.findOverlaps
        val message =
          s"Following schema criterions overlap in different groups (TSV, JSON, skip): " +
            s"${overlaps.map(_.asString).mkString(", ")}. " +
            s"Make sure every schema can have only one format"
        Either.cond(overlaps.isEmpty, config, message)
      }

  final case class Shredder(input: URI, output: URI, outputBad: URI, compression: Compression)

  sealed trait Compression extends StringEnum
  object Compression {
    final case object None extends Compression { val asString = "NONE" }
    final case object Gzip extends Compression { val asString = "GZIP" }
  }

  final case class Monitoring(snowplow: Option[SnowplowMonitoring], sentry: Option[Sentry])
  final case class SnowplowMonitoring(appId: String, collector: String)

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

  final case class Sentry(dsn: URI)

  implicit val shredderDecoder: Decoder[Shredder] =
    deriveDecoder[Shredder]

  implicit val uriDecoder: Decoder[URI] =
    Decoder[String].emap(s => Either.catchOnly[IllegalArgumentException](URI.create(s)).leftMap(_.toString))

  implicit val sentryDecoder: Decoder[Sentry] =
    deriveDecoder[Sentry]

  implicit val formatsDecoder: Decoder[Formats] =
    deriveDecoder[Formats]

  implicit val snowplowMonitoringDecoder: Decoder[SnowplowMonitoring] =
    deriveDecoder[SnowplowMonitoring]

  implicit val monitoringDecoder: Decoder[Monitoring] =
    deriveDecoder[Monitoring]

  implicit val decodeOutputCompression: Decoder[Compression] =
    StringEnum.decodeStringEnum[Compression]
  implicit val encodeOutputCompression: Encoder[Compression] =
    Encoder.instance(_.toString.toUpperCase.asJson)

  implicit val configDecoder: Decoder[Config[StorageTarget]] =
    deriveDecoder[Config[StorageTarget]]


  val formats: Lens[Config[StorageTarget], Formats] = GenLens[Config[StorageTarget]](_.formats)
  val monitoring: Lens[Config[StorageTarget], Monitoring] = GenLens[Config[StorageTarget]](_.monitoring)
}
