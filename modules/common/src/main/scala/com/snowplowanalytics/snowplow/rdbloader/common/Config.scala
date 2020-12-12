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
final case class Config[D <: StorageTarget](name: String,
                                            id: UUID,
                                            region: String,
                                            jsonpaths: Option[S3.Folder],
                                            compression: OutputCompression,
                                            monitoring: Monitoring,
                                            messageQueue: String,
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

  sealed trait OutputCompression extends StringEnum
  object OutputCompression {
    final case object None extends OutputCompression { val asString = "NONE" }
    final case object Gzip extends OutputCompression { val asString = "GZIP" }
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

    /** Final all criterion overlaps in two lists */
    def findOverlaps(as: List[SchemaCriterion], bs: List[SchemaCriterion]): Set[SchemaCriterion] =
      as.foldLeft(List.empty[(SchemaCriterion, SchemaCriterion)])(multiply(bs))
        .foldLeft(Set.empty[SchemaCriterion])(aggregateOverlapping)

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

    def aggregateOverlapping(acc: Set[SchemaCriterion], pair: (SchemaCriterion, SchemaCriterion)): Set[SchemaCriterion] = (acc, pair) match {
      case (acc, (a, b)) if overlap(a, b) => acc + a + b
      case (acc, _) => acc
    }

    def multiply[A](otherA: List[A])(acc: List[(A, A)], cur: A): List[(A, A)] =
      acc ++ otherA.zip(List.fill(otherA.length)(cur))
  }

  final case class Sentry(dsn: URI)

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

  implicit val decodeOutputCompression: Decoder[OutputCompression] =
    StringEnum.decodeStringEnum[OutputCompression]

  implicit val configDecoder: Decoder[Config[StorageTarget]] =
    deriveDecoder[Config[StorageTarget]]


  val formats: Lens[Config[StorageTarget], Formats] = GenLens[Config[StorageTarget]](_.formats)
  val monitoring: Lens[Config[StorageTarget], Monitoring] = GenLens[Config[StorageTarget]](_.monitoring)
}
