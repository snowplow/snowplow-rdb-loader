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
package com.snowplowanalytics.snowplow.rdbloader.loaders

import scala.concurrent.duration.FiniteDuration
import java.sql.Timestamp

import cats.data.State
import cats.effect.{Clock, Timer}
import cats.syntax.either._
import org.specs2.Specification

// This project
import com.snowplowanalytics.snowplow.rdbloader.{LoaderError, TestInterpreter, SpecHelpers, LoaderAction}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{AWS, Cache, Logging, JDBC}
import com.snowplowanalytics.snowplow.rdbloader.utils.S3
import com.snowplowanalytics.snowplow.rdbloader.utils.S3.{ Folder, Key }
import com.snowplowanalytics.snowplow.rdbloader.config.{ CliConfig, Step, Semver }
import com.snowplowanalytics.snowplow.rdbloader.db.{ Decoder, Entities }
import com.snowplowanalytics.snowplow.rdbloader.discovery.{ DataDiscovery, ShreddedType }
import com.snowplowanalytics.snowplow.rdbloader.loaders.Common.SqlString
import com.snowplowanalytics.snowplow.rdbloader.loaders.Common.SqlString.{unsafeCoerce => sql}

import com.snowplowanalytics.snowplow.rdbloader.SpecHelpers._
import com.snowplowanalytics.snowplow.rdbloader.TestInterpreter.{AWSResults, JDBCResults, ControlResults, TestState, Test}


class RedshiftLoaderSpec extends Specification { def is = s2"""
  Discover atomic events data and create load statements $e1
  Discover full data and create load statements $e2
  Do not fail on empty discovery $e3
  Do not sleep with disabled consistency check $e4
  Perform manifest-insertion and load within same transaction $e5
  Load Manifest check does not allow items with same etlTstamp $e6
  Transit copy creates and deletes a temporary table $e7
  """

  val noDiscovery = DataDiscovery(Folder.coerce("s3://noop"), None, None, Nil, false)


  def e1 = {
    def listBucket(bucket: Folder): Either[LoaderError, List[S3.BlobObject]] =
      List(
        // This should succeed for "atomicDiscovery"
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57_$folder$"),
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/_SUCCESS"),
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/$folder$"),
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-02")
      ).map(k => S3.BlobObject(k, 1L)).asRight

    implicit val timer: Timer[Test] = TestInterpreter.stateTimerInterpreter
    implicit val control: Logging[Test] = TestInterpreter.stateControlInterpreter(ControlResults.init)
    implicit val aws: AWS[Test] = TestInterpreter.stateAwsInterpreter(AWSResults.init.copy(listS3 = Test.liftWith(listBucket)))
    implicit val cache: Cache[Test] = TestInterpreter.stateCacheInterpreter

    val (_, result) = Common.discover[Test](CliConfig(validConfig, validTarget, Set.empty, None, None, false, SpecHelpers.resolverJson)).value.run(TestState.init).value

    val expected =
      List(DataDiscovery(S3.Folder.coerce("s3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/"), Some(1), Some(1L), Nil, specificFolder = false))

    result must beRight(expected)
  }

  def e2 = {
    val separator = "\t"

    val steps: Set[Step] = Step.defaultSteps ++ Set(Step.Vacuum)
    val discovery = DataDiscovery(
      S3.Folder.coerce("s3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/"),
      Some(3),
      None,
      List(
        ShreddedType.Json(
          ShreddedType.Info(Folder.coerce("s3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/"), "com.snowplowanalytics.snowplow", "submit_form", 1, Semver(0, 12, 0)),
          Key.coerce("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/submit_form_1.json")
        )
      ), specificFolder = false
    )
    val result = RedshiftLoadStatements.buildQueue(validConfig, validTarget, steps)(List(discovery))

    val atomic =
      s"""COPY atomic.events FROM 's3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/atomic-events/'
         | CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789876:role/RedshiftLoadRole'
         | REGION AS 'us-east-1'
         | MAXERROR 1
         | TIMEFORMAT 'auto'
         | DELIMITER '$separator'
         | EMPTYASNULL
         | FILLRECORD
         | TRUNCATECOLUMNS
         | ACCEPTINVCHARS """.stripMargin

    val vacuum = List(
      sql("VACUUM SORT ONLY atomic.events"),
      sql("VACUUM SORT ONLY atomic.com_snowplowanalytics_snowplow_submit_form_1"))

    val analyze = List(
      sql("ANALYZE atomic.events"),
      sql("ANALYZE atomic.com_snowplowanalytics_snowplow_submit_form_1"))

    val shredded = List(sql(
      """COPY atomic.com_snowplowanalytics_snowplow_submit_form_1 FROM 's3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-'
        | CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789876:role/RedshiftLoadRole' JSON AS 's3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/submit_form_1.json'
        | REGION AS 'us-east-1'
        | MAXERROR 1
        | TIMEFORMAT 'auto'
        | TRUNCATECOLUMNS
        | ACCEPTINVCHARS """.stripMargin))

    val manifest =
      """INSERT INTO atomic.manifest
        | SELECT etl_tstamp, sysdate AS commit_tstamp, count(*) AS event_count, 1 AS shredded_cardinality
        | FROM atomic.events
        | WHERE etl_tstamp IS NOT null
        | GROUP BY 1
        | ORDER BY etl_tstamp DESC
        | LIMIT 1""".stripMargin

    val expectedItem = RedshiftLoadStatements("atomic", RedshiftLoadStatements.StraightCopy(sql(atomic)), shredded, Some(vacuum), Some(analyze), sql(manifest), discovery)
    val expected = List(expectedItem)

    result must beEqualTo(expected)
  }

  def e3 = {
    implicit val control: Logging[Test] = TestInterpreter.stateControlInterpreter(ControlResults.init)
    implicit val jdbc: JDBC[Test] = TestInterpreter.stateJdbcInterpreter(JDBCResults.init)

    val steps: Set[Step] = Step.defaultSteps ++ Set(Step.Vacuum)
    val (_, result) = RedshiftLoader.run[Test](validConfig, validTarget, steps, Nil).value.run(TestState.init).value

    result must beRight
  }

  def e4 = {
    def keyExists(k: Key): Boolean =
      k == "s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/submit_form_1.json"

    def listBucket(bucket: Folder): Either[LoaderError, List[S3.BlobObject]] =
      List(
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-00001"),
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-00001"),
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/atomic-events/part-00001"),
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-0-0/part-00001-dbb35260-7b12-494b-be87-e7a4b1f59906.txt"),
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-0-0/part-00002-cba3a610-0b90-494b-be87-e7a4b1f59906.txt"),
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-0-0/part-00003-fba35670-9b83-494b-be87-e7a4b1f59906.txt"),
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-0-0/part-00004-fba3866a-8b90-494b-be87-e7a4b1fa9906.txt"),
        S3.Key.coerce(bucket + "run=2017-05-22-12-20-57/shredded-types/vendor=com.snowplowanalytics.snowplow/name=submit_form/format=jsonschema/version=1-0-0/part-00005-aba3568f-7b96-494b-be87-e7a4b1fa9906.txt")
      ).map(k => S3.BlobObject(k, 2L)).asRight

    implicit val control: Logging[Test] = TestInterpreter.stateControlInterpreter(ControlResults.init)
    implicit val aws: AWS[Test] = TestInterpreter.stateAwsInterpreter(AWSResults.init.copy(listS3 = Test.liftWith(listBucket), keyExists = keyExists))
    implicit val cache: Cache[Test] = TestInterpreter.stateCacheInterpreter
    implicit val timer: Timer[Test] = new Timer[Test] {
      def clock: Clock[Test] = TestInterpreter.testClock
      def sleep(duration: FiniteDuration): Test[Unit] = throw new RuntimeException("TODO")
    }

    val steps: Set[Step] = (Step.defaultSteps - Step.ConsistencyCheck) ++ Set(Step.Vacuum)
    val (_, result) = Common.discover[Test](CliConfig(validConfig, validTarget, steps, None, None, false, SpecHelpers.resolverJson)).value.run(TestState.init).value

    val expected = List(DataDiscovery(
      S3.Folder.coerce("s3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/"),
      Some(3),
      Some(6),
      List(
        ShreddedType.Json(
          ShreddedType.Info(Folder.coerce("s3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/"), "com.snowplowanalytics.snowplow", "submit_form", 1, Semver(0, 12, 0, Some(Semver.ReleaseCandidate(4)))),
          Key.coerce("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/submit_form_1.json")
        )
      ), specificFolder = false
    ))

    result must beRight(expected)
  }

  def e5 = {
    implicit val control: Logging[Test] = TestInterpreter.stateControlInterpreter(ControlResults.init.copy(print = ControlResults.noop))
    implicit val jdbc: JDBC[Test] = TestInterpreter.stateJdbcInterpreter(JDBCResults.init)

    val input = RedshiftLoadStatements(
      "atomic",
      RedshiftLoadStatements.StraightCopy(sql("LOAD INTO atomic MOCK")),
      List(sql("LOAD INTO SHRED 1 MOCK"), sql("LOAD INTO SHRED 2 MOCK"), sql("LOAD INTO SHRED 3 MOCK")),
      Some(List(sql("VACUUM MOCK"))),   // Must be shred cardinality + 1
      Some(List(sql("ANALYZE MOCK"))),
      sql("MANIFEST INSERT MOCK"),
      noDiscovery
    )

    val (state, result) = RedshiftLoader.loadFolder[Test](Step.defaultSteps - Step.LoadManifestCheck)(input).value.run(TestState.init).value

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

    val transactionsExpectation = state.getLog must beEqualTo(expected)
    val resultExpectation = result must beRight
    transactionsExpectation.and(resultExpectation)
  }

  def e6 = {
    val latestTimestamp = Timestamp.valueOf("2018-02-27 00:00:00.01")
    def executeQuery[A](query: SqlString)(implicit ev: Decoder[A]): LoaderAction[Test, A] = {
      val _ = ev
      val result: Option[Any] = if (query.contains("FROM atomic.events")) {
        Some(Entities.Timestamp(latestTimestamp))
      } else if (query.contains("FROM atomic.manifest")) {
        val commitTime = Timestamp.valueOf("2018-02-28 00:00:00.01")
        Some(Entities.LoadManifestItem(latestTimestamp, commitTime, 1000, 5))
      } else throw new RuntimeException("TODO")

      val state = State { log: TestState => (log.log(query), result.asInstanceOf[A].asRight[LoaderError]) }
      state.toAction
    }

    implicit val control: Logging[Test] = TestInterpreter.stateControlInterpreter(ControlResults.init.copy(print = ControlResults.noop))
    implicit val jdbc: JDBC[Test] = TestInterpreter.stateJdbcInterpreter(JDBCResults.init.copy(executeQuery = q => e => executeQuery(q)(e)))

    val shouldNot = "SHOULD NOT BE EXECUTED"

    val input = RedshiftLoadStatements(
      "atomic",
      RedshiftLoadStatements.StraightCopy(sql("LOAD INTO atomic.events MOCK")),
      List(sql(shouldNot), sql(shouldNot), sql(shouldNot)),
      Some(List(sql(shouldNot))),   // Must be shred cardinality + 1
      Some(List(sql(shouldNot))),
      sql(shouldNot),
      noDiscovery
    )

    val (state, result) = RedshiftLoader.loadFolder[Test](Step.defaultSteps)(input).value.run(TestState.init).value

    val expected = List(
      "BEGIN",
      "LOAD INTO atomic.events MOCK",
      "SELECT etl_tstamp FROM atomic.events WHERE etl_tstamp IS NOT null ORDER BY etl_tstamp DESC LIMIT 1",
      s"SELECT * FROM atomic.manifest WHERE etl_tstamp = '${latestTimestamp.toString}' ORDER BY etl_tstamp DESC LIMIT 1"
    )

    val transactionsExpectation = state.getLog must beEqualTo(expected)
    val resultExpectation = result must beLeft
    transactionsExpectation and resultExpectation
  }

  def e7 = {
    val latestTimestamp = Timestamp.valueOf("2018-02-26 00:00:01.000")
    def executeQuery[A](query: SqlString)(implicit ev: Decoder[A]): LoaderAction[Test, A] = {
      val _ = ev
      val result = if (query.contains("SELECT etl_tstamp")) {
        Some(Entities.Timestamp(latestTimestamp))
      } else if (query.contains("FROM atomic.manifest")) {
        val manifestEtlTime = Timestamp.valueOf("2018-02-27 00:00:01.00")
        val commitTime = Timestamp.valueOf("2018-02-28 00:00:01.000")
        Some(Entities.LoadManifestItem(manifestEtlTime, commitTime, 1000, 5))
      } else throw new RuntimeException("TODO")

      val state = State { log: TestState => (log.log(query), result.asInstanceOf[A].asRight[LoaderError]) }
      state.toAction
    }

    implicit val control: Logging[Test] = TestInterpreter.stateControlInterpreter(ControlResults.init.copy(print = ControlResults.noop))
    implicit val jdbc: JDBC[Test] = TestInterpreter.stateJdbcInterpreter(JDBCResults.init.copy(executeQuery = q => e => executeQuery(q)(e)))

    val expected = List(
      "BEGIN",
      "CREATE TABLE atomic.temp_transit_events ( LIKE atomic.events )",
      "SELECT etl_tstamp FROM atomic.temp_transit_events WHERE etl_tstamp IS NOT null ORDER BY etl_tstamp DESC LIMIT 1",
      s"SELECT * FROM atomic.manifest WHERE etl_tstamp = '${latestTimestamp.toString}' ORDER BY etl_tstamp DESC LIMIT 1",
      "COPY",
      "DROP TABLE atomic.temp_transit_events",

      "LOAD INTO SHRED 1 MOCK", "LOAD INTO SHRED 2 MOCK", "LOAD INTO SHRED 3 MOCK", "MANIFEST INSERT MOCK", "COMMIT", "END",
      "VACUUM MOCK", "BEGIN", "ANALYZE MOCK", "COMMIT"
    )

    val input = RedshiftLoadStatements(
      "atomic",
      RedshiftLoadStatements.TransitCopy(sql("COPY")),
      List(sql("LOAD INTO SHRED 1 MOCK"), sql("LOAD INTO SHRED 2 MOCK"), sql("LOAD INTO SHRED 3 MOCK")),
      Some(List(sql("VACUUM MOCK"))),   // Must be shred cardinality + 1
      Some(List(sql("ANALYZE MOCK"))),
      sql("MANIFEST INSERT MOCK"),
      noDiscovery
    )

    val (state, result) = RedshiftLoader.loadFolder[Test](Step.defaultSteps)(input).value.run(TestState.init).value

    val transactionsExpectation = state.getLog must beEqualTo(expected)
    val resultExpectation = result must beRight(())
    transactionsExpectation.and(resultExpectation)
  }
}

