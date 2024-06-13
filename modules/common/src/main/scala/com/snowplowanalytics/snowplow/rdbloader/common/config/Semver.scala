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
package com.snowplowanalytics.snowplow.rdbloader.common.config

import cats.implicits._
import cats.{PartialOrder, Show}

import io.circe.{Decoder, Encoder}
import io.circe.syntax._

import com.snowplowanalytics.snowplow.rdbloader.common.Common.IntString

/**
 * Snowplow-specific semantic versioning for apps, libs and jobs, with defined decoders and ordering
 * to compare different versions
 */
final case class Semver(
  major: Int,
  minor: Int,
  patch: Int,
  prerelease: Option[Semver.Prerelease]
)

/**
 * Helpers for Snowplow-specific semantic versioning
 */
object Semver {

  /**
   * Prerelease part of semantic version, mostly intended to be private Milestone is for libs,
   * release candidate for apps, unknown as last resort - will match any string
   */
  sealed trait Prerelease { def full: String }
  object Prerelease {
    final case class Milestone(version: Int) extends Prerelease { def full = s"-M$version" }
    final case class ReleaseCandidate(version: Int) extends Prerelease { def full = s"-rc$version" }
    final case class Unknown(full: String) extends Prerelease
  }

  val semverPattern    = """^(\d+)\.(\d+)\.(\d+)(.*)$""".r
  val milestonePattern = """-M(\d+)$""".r
  val rcPattern        = """-rc(\d+)$""".r

  /**
   * Alternative constructor to omit optional prerelease part
   */
  def apply(
    major: Int,
    minor: Int,
    patch: Int
  ): Semver =
    Semver(major, minor, patch, None)

  /**
   * Partial ordering instance for optional `Prerelease`. Can compare only same milestones with
   * milestones, rcs with rcs and final release with prerelease
   */
  private implicit val prereleaseOrder = new PartialOrder[Option[Prerelease]] {
    def partialCompare(x: Option[Prerelease], y: Option[Prerelease]): Double = (x, y) match {
      case (Some(_), None) => -1
      case (None, Some(_)) => 1
      case (None, None)    => 0
      case (Some(Prerelease.Milestone(xm)), Some(Prerelease.Milestone(ym))) =>
        xm.partialCompare(ym)
      case (Some(Prerelease.ReleaseCandidate(xrc)), Some(Prerelease.ReleaseCandidate(yrc))) =>
        xrc.partialCompare(yrc)
      case _ => Double.NaN
    }
  }

  /**
   * Partial ordering instance for semantic version Order isn't defined for prereleases of different
   * types, prerelease is always "less" than final release
   */
  implicit val semverOrder = new PartialOrder[Semver] {
    def partialCompare(x: Semver, y: Semver): Double =
      implicitly[PartialOrder[(Int, Int, Int, Option[Prerelease])]].partialCompare(
        (x.major, x.minor, x.patch, x.prerelease),
        (y.major, y.minor, y.patch, y.prerelease)
      )
  }

  /**
   * Decode `Prerelease` from string. Any string can be decoded as last-resort `Unknown`
   */
  def decodePrerelease(s: String): Prerelease = s match {
    case milestonePattern(IntString(m)) => Prerelease.Milestone(m)
    case rcPattern(IntString(rc))       => Prerelease.ReleaseCandidate(rc)
    case _                              => Prerelease.Unknown(s)
  }

  /**
   * Decode semantic version from string. First part must match X.Y.Z, last can be parsed either as
   * final release or prerelease
   */
  def decodeSemver(s: String): Either[String, Semver] = s match {
    case semverPattern(IntString(major), IntString(minor), IntString(patch), "") =>
      Right(Semver(major, minor, patch, None))
    case semverPattern(IntString(major), IntString(minor), IntString(patch), preprelease) =>
      Right(Semver(major, minor, patch, Some(decodePrerelease(preprelease))))
    case _ =>
      Left(s"Version [$s] doesn't match Semantic Version pattern")
  }

  private implicit val prereleaseShow = new Show[Option[Prerelease]] {
    def show(prerelease: Option[Prerelease]) = prerelease match {
      case Some(p) => p.full
      case None    => ""
    }
  }

  implicit val semverShow = new Show[Semver] {
    def show(version: Semver) =
      s"${version.major}.${version.minor}.${version.patch}${version.prerelease.show}"
  }

  /** Circe codecs for semantic version */
  implicit val semverDecoder: Decoder[Semver] =
    Decoder.decodeString.emap(decodeSemver)
  implicit val semverEncoder: Encoder[Semver] =
    Encoder.instance(ver => ver.show.asJson)
}
