/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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

import java.util.UUID
import java.time.Instant
import java.sql.Timestamp

import cats.{Id, ~>}
import cats.implicits._
import cats.data.{ State => _, _ }
import cats.effect._

import org.specs2.Specification

import com.snowplowanalytics.manifest.core._

// This project
import Common.SqlString.{unsafeCoerce => sql}
import interpreters.implementations.ManifestInterpreter.ManifestE
import S3.{ Folder, Key }
import config.{ CliConfig, Step, Semver }
import discovery.{ DataDiscovery, ShreddedType, ManifestDiscovery }


class RedshiftLoaderSpec extends Specification { def is = s2"""
  Discover atomic events data and create load statements $e1
  Discover full data and create load statements $e2
  Do not fail on empty discovery $e3
  Do not sleep with disabled consistency check $e4
  Perform manifest-insertion and load within same transaction $e5
  Load Manifest check does not allow items with same etlTstamp $e6
  Transit copy creates and deletes a temporary table $e7
  Perform load with discovery through processing manifest $e8
  Write an application_error failure for failed redshift load $e9
  """

  import SpecHelpers._

  val noDiscovery = DataDiscovery(Folder.coerce("s3://noop"), None, None, Nil, false, None)
  def newId = UUID.randomUUID()
  val time = Instant.now()

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
            ).map(k => S3.BlobObject(k, 1L)))

          case LoaderA.Sleep(_) => ()

          case action =>
            throw new RuntimeException(s"Unexpected Action [$action]")
        }
      }
    }

    val expected =
      List(DataDiscovery(S3.Folder.coerce("s3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/"), Some(1), Some(1L), Nil, specificFolder = false, None))

    val action = Common.discover(CliConfig(validConfig, validTarget, Set.empty, None, None, false, SpecHelpers.resolverJson))
    val result = action.value.foldMap(interpreter)

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
        ShreddedType(
          ShreddedType.Info(Folder.coerce("s3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/"), "com.snowplowanalytics.snowplow", "submit_form", 1, Semver(0, 12, 0)),
          Key.coerce("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/submit_form_1.json")
        )
      ), specificFolder = false, None
    )
    val result = RedshiftLoadStatements.buildQueue(validConfig, validTarget, steps)(List(discovery))

    val atomic =
      s"""COPY atomic.events FROM 's3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/atomic-events/'
         | CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789876:role/RedshiftLoadRole' REGION AS 'us-east-1'
         | DELIMITER '$separator' MAXERROR 1
         | EMPTYASNULL FILLRECORD TRUNCATECOLUMNS
         | TIMEFORMAT 'auto' ACCEPTINVCHARS """.stripMargin

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
        | MAXERROR 1 TRUNCATECOLUMNS TIMEFORMAT 'auto'
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
            ).map(k => S3.BlobObject(k, 2L)))

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
    val action = Common.discover(CliConfig(validConfig, validTarget, steps, None, None, false, SpecHelpers.resolverJson)).value
    val result: Either[LoaderError, List[DataDiscovery]] = action.foldMap(interpreter)

    val expected = List(DataDiscovery(
      S3.Folder.coerce("s3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/"),
      Some(3),
      Some(6),
      List(
        ShreddedType(
          ShreddedType.Info(Folder.coerce("s3://snowplow-acme-storage/shredded/good/run=2017-05-22-12-20-57/"), "com.snowplowanalytics.snowplow", "submit_form", 1, Semver(0, 12, 0, Some(Semver.ReleaseCandidate(4)))),
          Key.coerce("s3://snowplow-hosted-assets-us-east-1/4-storage/redshift-storage/jsonpaths/com.snowplowanalytics.snowplow/submit_form_1.json")
        )
      ), specificFolder = false, None
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
          case LoaderA.ExecuteUpdate(query) =>
            queries.append(query)
            Right(1L)

          case LoaderA.Print(_) =>
            ()

          case LoaderA.ExecuteQuery(_, _) =>
            Right(None)

          case action =>
            throw new RuntimeException(s"Unexpected Action [$action]")
        }
      }
    }

    val input = RedshiftLoadStatements(
      "atomic",
      RedshiftLoadStatements.StraightCopy(sql("LOAD INTO atomic MOCK")),
      List(sql("LOAD INTO SHRED 1 MOCK"), sql("LOAD INTO SHRED 2 MOCK"), sql("LOAD INTO SHRED 3 MOCK")),
      Some(List(sql("VACUUM MOCK"))),   // Must be shred cardinality + 1
      Some(List(sql("ANALYZE MOCK"))),
      sql("MANIFEST INSERT MOCK"),
      noDiscovery
    )

    val state = RedshiftLoader.loadFolder(Step.defaultSteps - Step.LoadManifestCheck)(input)
    val action = state.value
    val result = action.foldMap(interpreter)

    val transactionsExpectation = queries.toList must beEqualTo(expected)
    val resultExpectation = result must beRight
    transactionsExpectation.and(resultExpectation)
  }

  def e6 = {
    val expected = List(
      "BEGIN",
      "LOAD INTO atomic.events MOCK",
      "SELECT events",
      "SELECT manifest"
    )

    val queries = collection.mutable.ListBuffer.empty[String]

    def interpreter: LoaderA ~> Id = new (LoaderA ~> Id) {
      def apply[A](effect: LoaderA[A]): Id[A] = {
        effect match {
          case LoaderA.ExecuteUpdate(query) =>
            queries.append(query)
            Right(1L)

          case LoaderA.Print(_) =>
            ()

          case LoaderA.ExecuteQuery(query, _) if query.contains("FROM atomic.events") =>
            queries.append("SELECT events")
            val time = Timestamp.from(Instant.ofEpochMilli(1519757441133L))
            Right(Some(db.Entities.Timestamp(time)))

          case LoaderA.ExecuteQuery(query, _) if query.contains("FROM atomic.manifest") =>
            queries.append("SELECT manifest")
            val etlTime = Timestamp.from(Instant.ofEpochMilli(1519757441133L))
            val commitTime = Timestamp.from(Instant.ofEpochMilli(1519777441133L))
            Right(Some(db.Entities.LoadManifestItem(etlTime, commitTime, 1000, 5)))

          case action =>
            throw new RuntimeException(s"Unexpected Action [$action]")
        }
      }
    }

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

    val state = RedshiftLoader.loadFolder(Step.defaultSteps)(input)
    val action = state.value
    val result = action.foldMap(interpreter)

    val transactionsExpectation = queries.toList must beEqualTo(expected)
    val resultExpectation = result must beLeft
    transactionsExpectation and resultExpectation
  }

  def e7 = {
    val expected = List(
      "BEGIN",
      "CREATE TABLE atomic.temp_transit_events ( LIKE atomic.events )",
      "SELECT etl_tstamp", "SELECT manifest",
      "COPY",
      "DROP TABLE atomic.temp_transit_events",

      "LOAD INTO SHRED 1 MOCK", "LOAD INTO SHRED 2 MOCK", "LOAD INTO SHRED 3 MOCK", "MANIFEST INSERT MOCK", "COMMIT", "END",
      "VACUUM MOCK", "BEGIN", "ANALYZE MOCK", "COMMIT"
    )

    val queries = collection.mutable.ListBuffer.empty[String]

    def interpreter: LoaderA ~> Id = new (LoaderA ~> Id) {
      def apply[A](effect: LoaderA[A]): Id[A] = {
        effect match {
          case LoaderA.ExecuteUpdate(query) =>
            queries.append(query)
            Right(1L)

          case LoaderA.Print(_) =>
            ()

          case LoaderA.ExecuteQuery(query, _) if query.contains("FROM atomic.manifest") =>
            queries.append("SELECT manifest")
            val etlTime = Timestamp.from(Instant.ofEpochMilli(1519757441133L))
            val commitTime = Timestamp.from(Instant.ofEpochMilli(1519777441133L))
            Right(Some(db.Entities.LoadManifestItem(etlTime, commitTime, 1000, 5)))

          case LoaderA.ExecuteQuery(query, _) if query.contains("SELECT etl_tstamp") =>
            queries.append("SELECT etl_tstamp")
            val time = Timestamp.from(Instant.ofEpochMilli(1520164735L))
            Right(Some(db.Entities.Timestamp(time)))

          case action =>
            throw new RuntimeException(s"Unexpected Action [$action]")
        }
      }
    }

    val input = RedshiftLoadStatements(
      "atomic",
      RedshiftLoadStatements.TransitCopy(sql("COPY")),
      List(sql("LOAD INTO SHRED 1 MOCK"), sql("LOAD INTO SHRED 2 MOCK"), sql("LOAD INTO SHRED 3 MOCK")),
      Some(List(sql("VACUUM MOCK"))),   // Must be shred cardinality + 1
      Some(List(sql("ANALYZE MOCK"))),
      sql("MANIFEST INSERT MOCK"),
      noDiscovery
    )

    val state = RedshiftLoader.loadFolder(Step.defaultSteps)(input)
    val action = state.value
    val result = action.foldMap(interpreter)

    val transactionsExpectation = queries.toList must beEqualTo(expected)
    val resultExpectation = result must beRight(())
    transactionsExpectation.and(resultExpectation)
  }

  def e8 = {
    val expected = List(
      "BEGIN", "ACQUIRED",
      "COPY atomic.events", "SELECT etl_tstamp", "SELECT manifest",
      "COPY atomic.com_acme_context_1", "COPY atomic.com_acme_event_2", "COPY atomic.com_snowplowanalytics_snowplow_geolocation_context_1",
      "INSERT INTO atomic.manifest", "RELEASED",
      "COMMIT",

      "BEGIN",
      "ANALYZE atomic.events",
      "ANALYZE atomic.com_acme_context_1",
      "ANALYZE atomic.com_acme_event_2",
      "ANALYZE atomic.com_snowplowanalytics_snowplow_geolocation_context_1",
      "COMMIT"
    )

    val queries = collection.mutable.ListBuffer.empty[String]

    def interpreter: LoaderA ~> Id = new (LoaderA ~> Id) {
      val lockHandler = LockHandler[ManifestE](
        (_, _, _) => {
          queries.append(s"ACQUIRED")
          EitherT.pure[IO, ManifestError]((UUID.randomUUID(), Instant.now()))
        },
        (_, _, _, _) => {
          queries.append(s"RELEASED")
          EitherT.pure[IO, ManifestError]((UUID.randomUUID(), Instant.now()))
        },
        (_, _, id, _) => {
          queries.append(s"FAILED $id")
          EitherT.pure[IO, ManifestError]((UUID.randomUUID(), Instant.now()))
        }
      )

      def apply[A](effect: LoaderA[A]): Id[A] = {
        effect match {
          case LoaderA.ExecuteUpdate(query) if query.startsWith("COPY") =>
            queries.append(query.split(" ").take(2).mkString(" ").trim)
            Right(1L)

          case LoaderA.ExecuteUpdate(query) if query.startsWith("INSERT INTO atomic.manifest") =>
            queries.append("INSERT INTO atomic.manifest")
            Right(1L)

          case LoaderA.ExecuteUpdate(query) =>
            queries.append(query)
            Right(1L)

          case LoaderA.Print(_) =>
            ()

          case LoaderA.ExecuteQuery(query, _) if query.contains("FROM atomic.manifest") =>
            queries.append("SELECT manifest")
            val etlTime = Timestamp.from(Instant.ofEpochMilli(1519757441133L))
            val commitTime = Timestamp.from(Instant.ofEpochMilli(1519777441133L))
            Right(Some(db.Entities.LoadManifestItem(etlTime, commitTime, 1000, 5)))

          case LoaderA.ExecuteQuery(query, _) if query.contains("SELECT etl_tstamp") =>
            queries.append("SELECT etl_tstamp")
            val time = Timestamp.from(Instant.ofEpochMilli(1520164735L))
            Right(Some(db.Entities.Timestamp(time)))

          case LoaderA.Get(key) =>
            val result = "s3://snowplow-acme-storage/" ++ key
            Some(Some(Key.coerce(result)))

          case LoaderA.ManifestProcess(i, action) =>
            val proccess: ProcessingManifest.Process = (_: Item) => { action.value.foldMap(this) match {
              case Right(_) => scala.util.Success(None)
              case Left(e) => scala.util.Failure(LoaderError.LoaderThrowable(e))
            } }
            ProcessingManifest.processItemWithHandler[ManifestE](lockHandler)(Application("rdb-test", "0.1.0"), None, proccess)(i)
              .leftMap(LoaderError.fromManifestError)
              .value
              .unsafeRunSync()
              .void

          case action =>
            throw new RuntimeException(s"Unexpected Action [$action]")
        }
      }
    }

    val application = Application("snowplow-rdb-shredder", "0.14.0")
    val author = Author(application.agent, "0.1.0")
    val id1 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00001")
    val payload = getPayload("""["iglu:com.acme/context/jsonschema/1-0-0", "iglu:com.acme/event/jsonschema/2-0-0", "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-0"]""")
    val base = Folder.coerce("s3://snowplow-archive/shredded/run=2018-03-08-23-30-00/")
    val item = Item(NonEmptyList(
      Record(base, application, newId, None, State.New, time, author, None),
      List(
        Record(base, application, id1, None, State.Processing, time.plusSeconds(10), author, None),
        Record(base, application, newId, Some(id1), State.Processed, time.plusSeconds(20), author, payload)
      )))

    val dataDiscovery = for {
      info <- LoaderAction.liftE(ManifestDiscovery.parseItemPayload(item))
      data <- ManifestDiscovery.itemToDiscovery("us-east-1", None, item, info)
    } yield data

    val getStatements = RedshiftLoadStatements.getStatements(SpecHelpers.validConfig, SpecHelpers.validTargetWithManifest, Step.defaultSteps) _
    val statements = dataDiscovery.map(getStatements).value.foldMap(interpreter).right.toOption.get

    val state = RedshiftLoader.loadFolder(Step.defaultSteps)(statements)
    val result = state.value.foldMap(interpreter)

    val transactionsExpectation = queries.toList must beEqualTo(expected)
    val resultExpectation = result must beRight(())
    transactionsExpectation.and(resultExpectation)
  }

  def e9 = {
    val application = Application("snowplow-rdb-shredder", "0.14.0")
    val author = Author(application.agent, "0.1.0")
    val id1 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00001")
    val id2 = UUID.fromString("bdd54563-e419-431d-9695-19e738bf7735")
    val payload = getPayload("""["iglu:com.acme/context/jsonschema/1-0-0", "iglu:com.acme/event/jsonschema/2-0-0", "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-0"]""")
    val base = Folder.coerce("s3://snowplow-archive/shredded/run=2018-03-08-23-30-00/")
    val item = Item(NonEmptyList(
      Record(base, application, newId, None, State.New, time, author, None),
      List(
        Record(base, application, id1, None, State.Processing, time.plusSeconds(10), author, None),
        Record(base, application, newId, Some(id1), State.Processed, time.plusSeconds(20), author, payload)
      )))
    val discovery = DataDiscovery(base, Some(1), None, Nil, false, Some(item))
    val statements = RedshiftLoadStatements(
      "schem",
      RedshiftLoadStatements.StraightCopy("COPY to events table".sql),
      List("COPY to shredded1 table".sql),
      Some(List("VACUUM all tables".sql)),
      None,
      "COPY into manifest".sql,
      discovery
    )
    val result = RedshiftLoader.loadFolder(Step.defaultSteps)(statements)

    val queries = collection.mutable.ListBuffer.empty[String]

    def interpreter: LoaderA ~> Id = new (LoaderA ~> Id) {
      val lockHandler = LockHandler[ManifestE](
        (_, _, _) => {
          queries.append(s"ACQUIRED")
          EitherT.pure[IO, ManifestError]((id2, Instant.now()))
        },
        (_, _, _, _) => {
          queries.append(s"RELEASED")
          EitherT.pure[IO, ManifestError]((UUID.randomUUID(), Instant.now()))
        },
        (_, _, id, _) => {
          queries.append(s"FAILED $id")
          EitherT.pure[IO, ManifestError]((UUID.randomUUID(), Instant.now()))
        }
      )

      def apply[A](effect: LoaderA[A]): Id[A] = {
        effect match {
          case LoaderA.ExecuteUpdate(query) if query.startsWith("BEGIN") =>
            queries.append(query)
            Right(1L)

          case LoaderA.ExecuteUpdate(query) if query.startsWith("COPY") =>
            Left(LoaderError.StorageTargetError("Test failure"))

          case LoaderA.Print(m) =>
            ()

          case LoaderA.ManifestProcess(i, action) =>
            val proccess: ProcessingManifest.Process = (_: Item) => { action.value.foldMap(this) match {
              case Right(_) => throw new RuntimeException("Processing finished successfully")
              case Left(e) => scala.util.Failure(LoaderError.LoaderThrowable(e))
            } }
            ProcessingManifest.processItemWithHandler[ManifestE](lockHandler)(Application("rdb-test", "0.1.0"), None, proccess)(i)
              .leftMap(LoaderError.fromManifestError)
              .value
              .unsafeRunSync()
              .void

          case action =>
            throw new RuntimeException(s"Unexpected Action [$action]")
        }
      }
    }

    val resultExpectation = result.value.foldMap(interpreter) must beLeft
    val actionsExpectation = queries.toList must beEqualTo(List("BEGIN", "ACQUIRED", "FAILED bdd54563-e419-431d-9695-19e738bf7735"))

    resultExpectation and actionsExpectation
  }
}

