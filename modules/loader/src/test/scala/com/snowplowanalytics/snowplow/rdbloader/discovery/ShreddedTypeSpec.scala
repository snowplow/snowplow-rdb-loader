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
package com.snowplowanalytics.snowplow.rdbloader
package discovery

import cats.implicits._

import org.scalacheck.Gen
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

// This project
import com.snowplowanalytics.snowplow.rdbloader.dsl.{ Cache, AWS }
import com.snowplowanalytics.snowplow.rdbloader.common.{S3, LoaderMessage}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver

import com.snowplowanalytics.snowplow.rdbloader.test.{ Pure, PureCache, PureAWS, TestState }
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry

class ShreddedTypeSpec extends Specification with ScalaCheck {

  "extractSchemaKey" should {
    "parse a path for tabular output" >> {
      val input = "vendor=com.snowplow/name=event/format=tsv/model=1"
      ShreddedType.extractSchemaKey(input) must beSome(("com.snowplow", "event", 1, TypesInfo.Shredded.ShreddedFormat.TSV))
    }
  }

  "getSnowplowJsonPath" should {
    "put a result into cache only once" >> {
      implicit val cache: Cache[Pure] = PureCache.interpreter
      implicit val aws: AWS[Pure] = PureAWS.interpreter(PureAWS.init.withExistingKeys)

      val discoveryAction = ShreddedType.getSnowplowJsonPath[Pure]("eu-central-1", "foo_1.json")

      val (state, _) = discoveryAction.value.run(TestState.init).value

      state.getLog must beEqualTo(List(
        LogEntry.Message("PUT foo_1.json: Some(s3://snowplow-hosted-assets-eu-central-1/4-storage/redshift-storage/jsonpaths/foo_1.json)"),
      ))
    }
  }

  "discoverJsonPath" should {
    "respect the cache" >> {
      implicit val cache: Cache[Pure] = PureCache.interpreter
      implicit val aws: AWS[Pure] = PureAWS.interpreter(PureAWS.init.withExistingKeys)

      val info = ShreddedType.Info(S3.Folder.coerce("s3://some-bucket/"), "com.acme", "entity", 1, Semver(1,0,0), LoaderMessage.SnowplowEntity.SelfDescribingEvent)
      val discoveryAction = ShreddedType.discoverJsonPath[Pure]("eu-west-1", None, info)

      val (state, _) = (discoveryAction.flatMap(_ => discoveryAction)).value.run(TestState.init).value

      state.getLog must beEqualTo(List(
        LogEntry.Message("GET com.acme/entity_1.json (miss)"),
        LogEntry.Message("PUT com.acme/entity_1.json: Some(s3://snowplow-hosted-assets/4-storage/redshift-storage/jsonpaths/com.acme/entity_1.json)"),
        LogEntry.Message("GET com.acme/entity_1.json")
      ))
    }
  }
}

object ShreddedTypeSpec {

  /** `Gen` instance for a vendor/name-like string */
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
