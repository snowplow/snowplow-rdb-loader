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
import com.snowplowanalytics.snowplow.rdbloader.cloud.JsonPathDiscovery
import org.scalacheck.Gen
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

// This project
import com.snowplowanalytics.snowplow.rdbloader.dsl.Cache
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage

import com.snowplowanalytics.snowplow.rdbloader.test.{ Pure, PureCache, PureAWS, TestState }
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry

class TableTypeSpec extends Specification with ScalaCheck {

  "discoverJsonPath" should {
    "respect the cache" >> {
      implicit val cache: Cache[Pure] = PureCache.interpreter
      implicit val aws: BlobStorage[Pure] = PureAWS.blobStorage(PureAWS.init.withExistingKeys)
      val jsonPathDiscovery: JsonPathDiscovery[Pure] = JsonPathDiscovery.aws[Pure]("eu-west-1")

      val info = ShreddedType.Info(BlobStorage.Folder.coerce("s3://some-bucket/"), "com.acme", "entity", 1, LoaderMessage.SnowplowEntity.SelfDescribingEvent)
      val discoveryAction = jsonPathDiscovery.discoverJsonPath(None, info)

      val (state, _) = (discoveryAction.flatMap(_ => discoveryAction)).value.run(TestState.init).value

      state.getLog must beEqualTo(List(
        LogEntry.Message("GET com.acme/entity_1.json (miss)"),
        LogEntry.Message("PUT com.acme/entity_1.json: Some(s3://snowplow-hosted-assets/4-storage/redshift-storage/jsonpaths/com.acme/entity_1.json)"),
        LogEntry.Message("GET com.acme/entity_1.json")
      ))
    }
  }
}

object TableTypeSpec {

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
