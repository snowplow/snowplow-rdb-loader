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
package com.snowplowanalytics.snowplow.rdbloader.common

import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver

// specs2
import org.specs2.Specification

class SemverSpec extends Specification {
  def is = s2"""
  Decode valid semantic versions $e1
  Fail to decode invalid versions $e2
  Order valid semantic versions $e3
  """

  import Semver._

  def e1 = {
    val semverList = List("0.1.0-M1", "1.12.1-rc1", "0.0.1", "1.2.0", "10.10.10-rc8")
    val expected: List[Either[String, Semver]] = List(
      Semver(0, 1, 0, Some(Prerelease.Milestone(1))),
      Semver(1, 12, 1, Some(Prerelease.ReleaseCandidate(1))),
      Semver(0, 0, 1),
      Semver(1, 2, 0),
      Semver(10, 10, 10, Some(Prerelease.ReleaseCandidate(8)))
    ).map(Right.apply)

    val result = semverList.map(Semver.decodeSemver)
    result must beEqualTo(expected)
  }

  def e2 = {
    val invalidSemverList = List("1.0-M1", "-1.12.1-rc1", "0.2", "s.t.r", "-rc2")
    val result            = invalidSemverList.map(Semver.decodeSemver)

    result must contain((semver: Either[String, Semver]) => semver.isLeft).forall
  }

  def e3 = {
    val orders = List(
      Semver(0, 1, 0) < Semver(0, 2, 0),
      Semver(0, 2, 1) < Semver(0, 2, 2),
      Semver(1, 2, 1) > Semver(1, 2, 0),
      Semver(1, 2, 1, Some(Prerelease.ReleaseCandidate(1))) < Semver(1, 2, 1),
      Semver(1, 2, 1, Some(Prerelease.Milestone(1))) < Semver(1, 2, 1),
      Semver(1, 2, 1, Some(Prerelease.Unknown(""))) < Semver(1, 2, 1),
      Semver(1, 2, 1, Some(Prerelease.Unknown("bar"))) != Semver(1, 2, 1, Some(Prerelease.Unknown("foo"))),
      Semver(1, 2, 1, Some(Prerelease.Unknown("bar"))) == Semver(1, 2, 1, Some(Prerelease.Unknown("bar")))
    )

    orders must contain(true).forall
  }
}
