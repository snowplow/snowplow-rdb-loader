/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
package loaders

import cats.{Id, ~>}

import org.specs2.Specification

// This project
import Common.SqlString.{unsafeCoerce => sql}
import S3.{ Folder, Key }
import config.{ CliConfig, Step, Semver }

class RedshiftLoaderSpec extends Specification { def is = s2"""
  Discover atomic events data $e1
  Create load statements for full data discovery $e2
  Do not fail on empty discovery $e3
  Do not sleep with disabled consistency check $e4
  Perform manifest-insertion and load within same transaction $e5
  """

  import SpecHelpers._

  def e1 = {
    def interpreter: LoaderA ~> Id = new (LoaderA ~> Id) {
      def apply[A](effect: LoaderA[A]): Id[A] = {
        effect match {
          case LoaderA.ListS3(bucket) =>
            Right(List(
              // This should succeed for "atomicDiscovery"
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57_$folder$"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/_SUCCESS"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/$folder$"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-02")
            ))

          case LoaderA.Sleep(_) => ()

          case action =>
            throw new RuntimeException(s"Unexpected Action [$action]")
        }
      }
    }

    val expected =
      List(DataDiscovery.FullDiscovery( S3.Folder.coerce("s3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/"), 2, Nil))

    val action = Common.discover(CliConfig(validConfig, validTarget, Set.empty, None, None, false))
    val result = action.value.foldMap(interpreter)

    result must beRight(expected)
  }

  def e2 = {
    val separator = "\t"

    val steps: Set[Step] = Step.defaultSteps ++ Set(Step.Vacuum)
    val discovery = DataDiscovery.FullDiscovery(
      S3.Folder.coerce("s3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/"), 3,
      List(
        ShreddedType(
          ShreddedType.Info(Folder.coerce("s3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/"), "com.snowplowanalytics.snowplow", "submit_form", 1, Semver(0, 12, 0)),
          Key.coerce("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/submit_form_1.json")
        )
      )
    )
    val result = RedshiftLoadStatements.buildQueue(validConfig, validTarget, steps)(List(discovery))

    val atomic = s"""
         |COPY atomic.events FROM 's3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/atomic-events/'
         | CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789876:role/RedshiftLoadRole' REGION AS 'us-east-1'
         | DELIMITER '$separator' MAXERROR 1
         | EMPTYASNULL FILLRECORD TRUNCATECOLUMNS
         | TIMEFORMAT 'auto' ACCEPTINVCHARS ;""".stripMargin

    val vacuum = List(
      sql("VACUUM SORT ONLY atomic.events;"),
      sql("VACUUM SORT ONLY atomic.com_snowplowanalytics_snowplow_submit_form_1;"))

    val analyze = List(
      sql("ANALYZE atomic.events;"),
      sql("ANALYZE atomic.com_snowplowanalytics_snowplow_submit_form_1;"))

    val shredded = List(sql("""
        |COPY atomic.com_snowplowanalytics_snowplow_submit_form_1 FROM 's3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-'
        | CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789876:role/RedshiftLoadRole' JSON AS 's3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/submit_form_1.json'
        | REGION AS 'us-east-1'
        | MAXERROR 1 TRUNCATECOLUMNS TIMEFORMAT 'auto'
        | ACCEPTINVCHARS ;""".stripMargin))

    val manifest = """
        |INSERT INTO atomic.manifest
        | SELECT etl_tstamp, sysdate AS commit_tstamp, count(*) AS event_count, 1 AS shredded_cardinality
        | FROM atomic.events
        | WHERE etl_tstamp IS NOT null
        | GROUP BY 1
        | ORDER BY etl_tstamp DESC
        | LIMIT 1;""".stripMargin

    val expected = List(RedshiftLoadStatements(
      sql(atomic), shredded, Some(vacuum), Some(analyze), sql(manifest), Folder.coerce("s3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/")

    ))

    result must beEqualTo(expected)
  }

  def e3 = {
    def interpreter: LoaderA ~> Id = new (LoaderA ~> Id) {
      def apply[A](effect: LoaderA[A]): Id[A] = {
        effect match {
          case LoaderA.ListS3(_) => Right(Nil)

          case LoaderA.KeyExists(_) => false

          case LoaderA.Sleep(_) => ()

          case action =>
            throw new RuntimeException(s"Unexpected Action [$action]")
        }
      }
    }

    val steps: Set[Step] = Step.defaultSteps ++ Set(Step.Vacuum)
    val action = RedshiftLoader.run(validConfig, validTarget, steps, Nil)
    val result = action.value.foldMap(interpreter)

    result must beRight
  }

  def e4 = {
    def interpreter: LoaderA ~> Id = new (LoaderA ~> Id) {

      private val cache = collection.mutable.HashMap.empty[String, Option[S3.Key]]

      def apply[A](effect: LoaderA[A]): Id[A] = {
        effect match {
          case LoaderA.ListS3(bucket) =>
            Right(List(
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-00001"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-00001"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-00001"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-0-0/part-00001-dbb35260-7b12-494b-be87-e7a4b1f59906.txt"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-0-0/part-00002-cba3a610-0b90-494b-be87-e7a4b1f59906.txt"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-0-0/part-00003-fba35670-9b83-494b-be87-e7a4b1f59906.txt"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-0-0/part-00004-fba3866a-8b90-494b-be87-e7a4b1fa9906.txt"),
              S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-0-0/part-00005-aba3568f-7b96-494b-be87-e7a4b1fa9906.txt")
            ))

          case LoaderA.Get(key: String) =>
            cache.get(key)
          case LoaderA.Put(key: String, value: Option[S3.Key]) =>
            val _ = cache.put(key, value)
            ()

          case LoaderA.KeyExists(k) =>
            if (k == "s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/submit_form_1.json") {
              true
            } else false

          case LoaderA.Sleep(time) =>
            throw new RuntimeException(s"Data-discovery should not sleep with skipped consistency check. Sleep called for [$time]")

          case action =>
            throw new RuntimeException(s"Unexpected Action [$action]")
        }
      }
    }

    val steps: Set[Step] = (Step.defaultSteps - Step.ConsistencyCheck) ++ Set(Step.Vacuum)
    val action = Common.discover(CliConfig(validConfig, validTarget, steps, None, None, false)).value
    val result: Either[LoaderError, List[DataDiscovery]] = action.foldMap(interpreter)

    val expected = List(DataDiscovery.FullDiscovery(
      S3.Folder.coerce("s3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/"), 3,
      List(
        ShreddedType(
          ShreddedType.Info(Folder.coerce("s3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/"), "com.snowplowanalytics.snowplow", "submit_form", 1, Semver(0, 12, 0, Some(Semver.ReleaseCandidate(4)))),
          Key.coerce("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/submit_form_1.json")
        )
      )
    ))

    result must beRight(expected)
  }

  def e5 = {

    val expected = List(
      "BEGIN",
      "LOAD INTO atomic MOCK",
      "LOAD INTO SHRED 1 MOCK",
      "LOAD INTO SHRED 2 MOCK",
      "LOAD INTO SHRED 3 MOCK",
      "MANIFEST INSERT MOCK",
      "COMMIT",
      "END",
      "VACUUM MOCK",
      "BEGIN",
      "ANALYZE MOCK",
      "COMMIT"
    )

    val queries = collection.mutable.ListBuffer.empty[String]

    def interpreter: LoaderA ~> Id = new (LoaderA ~> Id) {
      def apply[A](effect: LoaderA[A]): Id[A] = {
        effect match {
          case LoaderA.ExecuteQuery(query) =>
            queries.append(query)
            Right(1L)

          case LoaderA.Print(_) =>
            ()

          case action =>
            throw new RuntimeException(s"Unexpected Action [$action]")
        }
      }
    }

    val input = RedshiftLoadStatements(
      sql("LOAD INTO atomic MOCK"),
      List(sql("LOAD INTO SHRED 1 MOCK"), sql("LOAD INTO SHRED 2 MOCK"), sql("LOAD INTO SHRED 3 MOCK")),
      Some(List(sql("VACUUM MOCK"))),   // Must be shred cardinality + 1
      Some(List(sql("ANALYZE MOCK"))),
      sql("MANIFEST INSERT MOCK"),
      Folder.coerce("s3://noop")
    )

    val state = RedshiftLoader.loadFolder(input)
    val action = state.value
    val result = action.foldMap(interpreter)

    val transactionsExpectation = queries.toList must beEqualTo(expected)
    val resultExpectation = result must beRight
    transactionsExpectation.and(resultExpectation)
  }
}

