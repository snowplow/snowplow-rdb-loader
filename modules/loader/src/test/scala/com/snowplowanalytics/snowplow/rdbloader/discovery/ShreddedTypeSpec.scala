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
package com.snowplowanalytics.snowplow.rdbloader
package discovery

import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.common.{S3, Semver}

import org.scalacheck.Gen
import org.specs2.{Specification, ScalaCheck}

// This project
import com.snowplowanalytics.snowplow.rdbloader.discovery.ShreddedType._

class ShreddedTypeSpec extends Specification with ScalaCheck { def is = s2"""
  Fail to transform path without valid vendor $e1
  Fail to transform path without file $e2
  Transform correct S3 path for Shred job $e3
  Transform full modern shredded key $e4
  extractedSchemaKey parsed a path for tabular output $e5
  Transform correct tabular S3 path $e6
  """

  def e1 = {
    val path = "cross-batch-test/shredded-archive/run%3D2017-04-27-14-39-42/submit_form/jsonschema/1-0-0/part-00000-00001"
    val key = S3.Key.coerce(s"s3://rdb-test/$path")
    val result = ShreddedType.transformPath(key, Semver(0,10,0))
    result must beLeft
  }

  def e2 = {
    val path = "cross-batch-test/shredded-archive/run%3D2017-04-27-14-39-42/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0"
    val key = S3.Key.coerce(s"s3://rdb-test/$path")
    val result = ShreddedType.transformPath(key, Semver(0,12,0))
    result must beLeft
  }

  def e3 = {
    val path = "vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-0-0/part-00000-00001"
    val key = S3.Key.coerce(s"s3://rdb-test/shredded-types/$path")
    val result = ShreddedType.transformPath(key, Semver(0,13,0))
    val expected = (false, Info(S3.Folder.coerce("s3://rdb-test"), "com.snowplowanalytics.snowplow", "submit_form", 1, Semver(0,13,0)))
    result must beRight(expected)
  }

  def e4 = {
    val key = S3.Key.coerce("s3://snowplow-shredded/good/run=2017-06-14-12-07-11/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-0-0/part-00000-00001")

    val expectedPrefix = S3.Folder.coerce("s3://snowplow-shredded/good/run=2017-06-14-12-07-11/")
    val expected = (false, Info(expectedPrefix, "com.snowplowanalytics.snowplow", "submit_form", 1, Semver(0,13,0)))

    val result = ShreddedType.transformPath(key, Semver(0,13,0))
    result must beRight(expected)
  }

  def e5 = {
    val input = "shredded-tsv/vendor=com.snowplow/name=event/format=jsonschema/version=1"
    ShreddedType.extractSchemaKey(input) must beSome(Extracted.Tabular("com.snowplow", "event", "jsonschema", 1))
  }

  def e6 = {
    val key = S3.Key.coerce("s3://snowplow-shredded/good/run=2017-06-14-12-07-11/shredded-tsv/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1/part-00000-00001")

    val expectedPrefix = S3.Folder.coerce("s3://snowplow-shredded/good/run=2017-06-14-12-07-11/")
    val expected = (true, Info(expectedPrefix, "com.snowplowanalytics.snowplow", "submit_form", 1, Semver(0,16,0)))

    val result = ShreddedType.transformPath(key, Semver(0,16,0))
    result must beRight(expected)
  }
}

object ShreddedTypeSpec {

  /**
    * `Gen` instance for a vendor/name-like string
    */
  implicit val alphaNum: Gen[String] = for {
    n <- Gen.chooseNum(1, 5)
    d <- Gen.oneOf('_', '.', '-')
    s <- Gen.listOf(Gen.alphaNumChar)
      .map(_.mkString)
      .suchThat(_.nonEmpty)
    (a, b) = s.splitAt(n)
    r <- Gen.const(s"$a$d$b")
      .suchThat(x => !x.startsWith(d.toString))
      .suchThat(x => !x.endsWith(d.toString))
  } yield r

  implicit val subpath: Gen[String] = for {
    s <- Gen.listOf(Gen.listOf(Gen.alphaNumChar).map(_.mkString).suchThat(_.nonEmpty))
    path = s.mkString("/")
  } yield if (path.isEmpty) "" else path + "/"

  /**
    * Generator of `ShreddedTypeElements`
    * This generator doesn't guarantee that all elements are valid
    * (such as `name` without dots), it allows to test parse failures
    */
  val shreddedTypeElementsGen = for {
    subpath <- subpath
    vendor <- alphaNum
    name <- alphaNum
    format <- alphaNum
    model <- Gen.chooseNum(0, 10)
    revision <- Gen.chooseNum(0, 10)
    addition <- Gen.chooseNum(0, 10)
  } yield (subpath, vendor, name, format, model, revision, addition)
}
