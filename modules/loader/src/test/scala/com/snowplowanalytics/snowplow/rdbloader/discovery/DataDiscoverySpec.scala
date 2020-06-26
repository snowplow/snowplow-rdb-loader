/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.discovery

import java.util.UUID

import cats.syntax.either._
import org.specs2.Specification
import com.snowplowanalytics.snowplow.rdbloader.{LoaderError, TestInterpreter}
import com.snowplowanalytics.snowplow.rdbloader.utils.S3.Folder.{coerce => dir}
import com.snowplowanalytics.snowplow.rdbloader.utils.S3.Key.{coerce => s3key}
import com.snowplowanalytics.snowplow.rdbloader.TestInterpreter.{AWSResults, ControlResults, Test, TestState}
import com.snowplowanalytics.snowplow.rdbloader.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.discovery.ShreddedType._
import com.snowplowanalytics.snowplow.rdbloader.dsl.{AWS, Cache, Logging}
import com.snowplowanalytics.snowplow.rdbloader.utils.S3


class DataDiscoverySpec extends Specification { def is = s2"""
  Successfully discover two run folders at once $e1
  Fail to proceed with empty target folder $e3
  Do not fail to proceed with empty shredded good folder $e4
  Successfully discover data in run folder $e5
  Successfully discover data in specific folder $e6
  Successfully discover several folders using `InSpecificFolder` (decide if desired) $e7
  listGoodBucket ignores special files $e8
  show DataDiscovery with several shredded types $e9
  """

  val id = UUID.fromString("8ad6fc06-ae5c-4dfc-a14d-f2ae86755179")

  def e1 = {
    def listS3(bucket: S3.Folder) =
      List(
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-0000"),
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-0001"),
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/1-0-0/part-00001"),
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/1-0-0/part-00002"),
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/2-0-0/part-00001"),

        S3.Key.coerce(bucket + "run=2017-05-22-16-00-57/atomic-events/part-0000"),
        S3.Key.coerce(bucket + "run=2017-05-22-16-00-57/atomic-events/part-0001"),
        S3.Key.coerce(bucket + "run=2017-05-22-16-00-57/com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0/part-00000"),
        S3.Key.coerce(bucket + "run=2017-05-22-16-00-57/com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0/part-00001")
      ).map(k => S3.BlobObject(k, 1L)).asRight[LoaderError]

    def keyExists(k: S3.Key): Boolean = {
      k.toString match {
        case "s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_1.json" => true
        case "s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_2.json" => true
        case "s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/add_to_cart_1.json" => true
        case _ => false
      }
    }

    implicit val control: Logging[Test] = TestInterpreter.stateControlInterpreter(ControlResults.init)
    implicit val aws: AWS[Test] = TestInterpreter.stateAwsInterpreter(AWSResults.init.copy(listS3 = Test.liftWith(listS3), keyExists = keyExists))
    implicit val cache: Cache[Test] = TestInterpreter.stateCacheInterpreter

    val shreddedGood = S3.Folder.coerce("s3://runfolder-test/shredded/good/")
    val discoveryTarget = DataDiscovery.Global(shreddedGood)
    val (state, result) = DataDiscovery.discover[Test](discoveryTarget, Semver(0,11,0), "us-east-1", None).value.run(TestState.init).value

    val expected = List(
      DataDiscovery(
        dir("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/"),
        Some(2L),
        Some(2L),
        List(
          ShreddedType.Json(
            Info(dir("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/"),"com.mailchimp","email_address_change",2,Semver(0,11,0,None)),
            s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_2.json")),
          ShreddedType.Json(
            Info(dir("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/"),"com.mailchimp","email_address_change",1,Semver(0,11,0,None)),
            s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_1.json"))
        ),
        specificFolder = false
      ),

      DataDiscovery(
        dir("s3://runfolder-test/shredded/good/run=2017-05-22-16-00-57/"),
        Some(2L),
        Some(2L),
        List(
          ShreddedType.Json(
            Info(dir("s3://runfolder-test/shredded/good/run=2017-05-22-16-00-57/"), "com.snowplowanalytics.snowplow","add_to_cart",1,Semver(0,11,0,None)),
            s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/add_to_cart_1.json"))
        ),
        specificFolder = false
      )
    )

    result must beRight(expected)
  }

  def e3 = {

    implicit val cache: Cache[Test] = TestInterpreter.stateCacheInterpreter
    implicit val control: Logging[Test] = TestInterpreter.stateControlInterpreter(ControlResults.init)
    implicit val aws: AWS[Test] = TestInterpreter.stateAwsInterpreter(AWSResults.init)

    val shreddedGood = S3.Folder.coerce("s3://runfolder-test/shredded/good/run=2017-08-21-19-18-20")

    val expected = LoaderError.DiscoveryError(List(DiscoveryFailure.NoDataFailure(shreddedGood)))

    val discoveryTarget = DataDiscovery.InSpecificFolder(shreddedGood)
    val (state, result) = DataDiscovery.discover[Test](discoveryTarget, Semver(0,11,0), "us-east-1", None).value.run(TestState.init).value

    result must beLeft(expected)
  }

  def e4 = {
    implicit val cache: Cache[Test] = TestInterpreter.stateCacheInterpreter
    implicit val control: Logging[Test] = TestInterpreter.stateControlInterpreter(ControlResults.init)
    implicit val aws: AWS[Test] = TestInterpreter.stateAwsInterpreter(AWSResults.init)

    val shreddedGood = S3.Folder.coerce("s3://runfolder-test/shredded/good")

    val expected = List.empty[DataDiscovery]

    // The only difference with e3
    val discoveryTarget = DataDiscovery.Global(shreddedGood)
    val (state, result) = DataDiscovery.discover[Test](discoveryTarget, Semver(0,11,0), "us-east-1", None).value.run(TestState.init).value

    result must beRight(expected)
  }

  def e5 = {
    def listS3(bucket: S3.Folder) =
      List(
        S3.Key.coerce(bucket + "atomic-events/part-0000"),
        S3.Key.coerce(bucket + "atomic-events/part-0001"),
        S3.Key.coerce(bucket + "com.mailchimp/email_address_change/jsonschema/1-0-0/part-00001"),
        S3.Key.coerce(bucket + "com.mailchimp/email_address_change/jsonschema/1-0-0/part-00002"),
        S3.Key.coerce(bucket + "com.mailchimp/email_address_change/jsonschema/2-0-0/part-00001")
      ).map(k => S3.BlobObject(k, 1L)).asRight[LoaderError]

    def keyExists(k: S3.Key): Boolean =
      k.toString match {
        case "s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_1.json" => true
        case "s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_2.json" => true
        case _ => false
      }

    implicit val control: Logging[Test] = TestInterpreter.stateControlInterpreter(ControlResults.init)
    implicit val aws: AWS[Test] = TestInterpreter.stateAwsInterpreter(AWSResults.init.copy(listS3 = Test.liftWith(listS3), keyExists = keyExists))
    implicit val cache: Cache[Test] = TestInterpreter.stateCacheInterpreter

    val expected = List(
      DataDiscovery(
        dir("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/"),
        Some(2L),
        Some(2L),
        List(
          ShreddedType.Json(
            Info(dir("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/"),"com.mailchimp","email_address_change",2,Semver(0,11,0,None)),
            s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_2.json")),
          ShreddedType.Json(
            Info(dir("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/"),"com.mailchimp","email_address_change",1,Semver(0,11,0,None)),
            s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_1.json"))
        ),
        specificFolder = false
      )
    )

    val shreddedGood = S3.Folder.coerce("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/")
    val discoveryTarget = DataDiscovery.Global(shreddedGood)
    val (state, result) = DataDiscovery.discover[Test](discoveryTarget, Semver(0,11,0), "us-east-1", None).value.run(TestState.init).value

    result must beRight(expected)
  }

  def e6 = {
    def listS3(bucket: S3.Folder) =
      List(
        S3.Key.coerce(bucket + "atomic-events/part-0000"),
        S3.Key.coerce(bucket + "atomic-events/part-0001"),
        S3.Key.coerce(bucket + "com.mailchimp/email_address_change/jsonschema/1-0-0/part-00001"),
        S3.Key.coerce(bucket + "com.mailchimp/email_address_change/jsonschema/1-0-0/part-00002"),
        S3.Key.coerce(bucket + "com.mailchimp/email_address_change/jsonschema/2-0-0/part-00001")
      ).map(k => S3.BlobObject(k, 1L)).asRight[LoaderError]

    def keyExists(k: S3.Key): Boolean =
      k.toString match {
        case "s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_1.json" => true
        case "s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_2.json" => true
        case _ => false
      }

    implicit val control: Logging[Test] = TestInterpreter.stateControlInterpreter(ControlResults.init)
    implicit val aws: AWS[Test] = TestInterpreter.stateAwsInterpreter(AWSResults.init.copy(listS3 = Test.liftWith(listS3), keyExists = keyExists))
    implicit val cache: Cache[Test] = TestInterpreter.stateCacheInterpreter

    val targetFolder = S3.Folder.coerce("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/")

    val expected = List(
      DataDiscovery(
        dir("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/"),
        Some(2L),
        Some(2L),
        List(
          ShreddedType.Json(
            Info(dir("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/"),"com.mailchimp","email_address_change",2,Semver(0,11,0,None)),
            s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_2.json")),
          ShreddedType.Json(
            Info(dir("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/"),"com.mailchimp","email_address_change",1,Semver(0,11,0,None)),
            s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_1.json"))
        ),
        specificFolder = true
      )
    )

    val discoveryTarget = DataDiscovery.InSpecificFolder(targetFolder)
    val (state, result) = DataDiscovery.discover[Test](discoveryTarget, Semver(0,11,0), "us-east-1", None).value.run(TestState.init).value

    result must beRight(expected)
  }

  def e7 = {
    def listS3(bucket: S3.Folder) =
      List(
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-0000"),
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-0001"),
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/1-0-0/part-00001"),
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/1-0-0/part-00002"),
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/com.mailchimp/email_address_change/jsonschema/2-0-0/part-00001"),
        // Another folder
        S3.Key.coerce(bucket + "run=2018-10-12-10-20-00/atomic-events/part-0000"),
        S3.Key.coerce(bucket + "run=2018-10-12-10-20-00/atomic-events/part-0001")
    ).map(k => S3.BlobObject(k, 1L)).asRight[LoaderError]

    def keyExists(k: S3.Key): Boolean =
      k.toString match {
        case "s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_1.json" => true
        case "s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_2.json" => true
        case _ => false
      }


    implicit val control: Logging[Test] = TestInterpreter.stateControlInterpreter(ControlResults.init)
    implicit val aws: AWS[Test] = TestInterpreter.stateAwsInterpreter(AWSResults.init.copy(listS3 = Test.liftWith(listS3), keyExists = keyExists))
    implicit val cache: Cache[Test] = TestInterpreter.stateCacheInterpreter

    val targetFolder = S3.Folder.coerce("s3://runfolder-test/shredded/good/")

    val expected = List(
      DataDiscovery(
        dir("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/"),
        Some(2L),
        Some(2L),
        List(
          ShreddedType.Json(
            Info(dir("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/"),"com.mailchimp","email_address_change",2,Semver(0,11,0,None)),
            s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_2.json")),
          ShreddedType.Json(
            Info(dir("s3://runfolder-test/shredded/good/run=2017-05-22-12-20-57/"),"com.mailchimp","email_address_change",1,Semver(0,11,0,None)),
            s3key("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.mailchimp/email_address_change_1.json"))
        ),
        specificFolder = true
      ),
      DataDiscovery(
        dir("s3://runfolder-test/shredded/good/run=2018-10-12-10-20-00/"),
        Some(2L),
        Some(2L),
        List(),
        specificFolder = true
      )
    )

    val discoveryTarget = DataDiscovery.InSpecificFolder(targetFolder)
    val (state, result) = DataDiscovery.discover[Test](discoveryTarget, Semver(0,11,0), "us-east-1", None).value.run(TestState.init).value

    result must beRight(expected)
  }

  def e8 = {
    def listS3(bucket: S3.Folder) =
      List(
        S3.BlobObject(S3.Key.join(bucket, "_SUCCESS"), 0L),
        S3.BlobObject(S3.Key.join(bucket, "part-00000-8e95d7a6-4c5f-4dd3-ab78-6ca8b8cef5d4-c000.txt.gz"), 20L),
        S3.BlobObject(S3.Key.join(bucket, "part-00001-8e95d7a6-4c5f-4dd3-ab78-6ca8b8cef5d4-c000.txt.gz"), 20L),
        S3.BlobObject(S3.Key.join(bucket, "part-00002-8e95d7a6-4c5f-4dd3-ab78-6ca8b8cef5d4-c000.txt.gz"), 20L)
      ).asRight[LoaderError]

    implicit val aws: AWS[Test] = TestInterpreter.stateAwsInterpreter(AWSResults.init.copy(listS3 = Test.liftWith(listS3)))

    val prefix = S3.Folder.coerce("s3://sp-com-acme-123987939231-10-batch-archive/main/shredded/good/run=2018-07-05-00-55-16/atomic-events/")

    val (state, result) = DataDiscovery.listGoodBucket[Test](prefix).value.run(TestState.init).value

    result.map(_.length) must beRight(3)
  }

  def e9 = {
    val shreddedTypes = List(
      ShreddedType.Json(ShreddedType.Info(S3.Folder.coerce("s3://my-bucket/my-path"), "com.acme", "event", 2, Semver(1,5,0)), S3.Key.coerce("s3://assets/event_1.json")),
      ShreddedType.Json(ShreddedType.Info(S3.Folder.coerce("s3://my-bucket/my-path"), "com.acme", "context", 2, Semver(1,5,0)), S3.Key.coerce("s3://assets/context_1.json"))
    )

    val discovery = DataDiscovery(S3.Folder.coerce("s3://my-bucket/my-path"), Some(8), Some(1024), shreddedTypes, false)
    discovery.show must beEqualTo(
      """|my-path with 8 atomic files (0 Mb) and with following shredded types:
         |  * iglu:com.acme/event/jsonschema/2-*-* (s3://assets/event_1.json)
         |  * iglu:com.acme/context/jsonschema/2-*-* (s3://assets/context_1.json)""".stripMargin)
  }
}
