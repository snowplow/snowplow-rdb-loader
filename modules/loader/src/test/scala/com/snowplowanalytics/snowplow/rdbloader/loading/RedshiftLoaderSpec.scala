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
package com.snowplowanalytics.snowplow.rdbloader.loading

// This project
import com.snowplowanalytics.snowplow.rdbloader.SpecHelpers
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, JDBC}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.SqlString.{unsafeCoerce => sql}

import com.snowplowanalytics.snowplow.rdbloader.common.config.{ Semver, Step }
import com.snowplowanalytics.snowplow.rdbloader.common.config.Config.Shredder.Compression

import com.snowplowanalytics.snowplow.rdbloader.test.{PureJDBC, Pure, PureLogging}

import org.specs2.mutable.Specification

class RedshiftLoaderSpec extends Specification {
  "loadFolder" should {
    "perform atomic and shredded insertions, ignore VACUUM and ANALYZE" >> {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(PureLogging.init.copy(print = PureLogging.noop))
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)

      val input = RedshiftStatements(
        "atomic",
        RedshiftStatements.AtomicCopy.Straight(sql("LOAD INTO atomic MOCK")),
        List(sql("LOAD INTO SHRED 1 MOCK"), sql("LOAD INTO SHRED 2 MOCK"), sql("LOAD INTO SHRED 3 MOCK")),
        Some(List(sql("VACUUM MOCK"))),
        Some(List(sql("ANALYZE MOCK")))
      )

      val (state, result) = RedshiftLoader.loadFolder[Pure](input).run

      val expected = List(
        "LOAD INTO atomic MOCK",
        "LOAD INTO SHRED 1 MOCK",
        "LOAD INTO SHRED 2 MOCK",
        "LOAD INTO SHRED 3 MOCK",
      )

      val transactionsExpectation = state.getLog must beEqualTo(expected)
      val resultExpectation = result must beRight
      transactionsExpectation.and(resultExpectation)
    }

    "perform atomic transit load with shredded types, ignore VACUUM and ANALYZE" >> {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(PureLogging.init.copy(print = PureLogging.noop))
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)

      val input = RedshiftStatements(
        "schema",
        RedshiftStatements.AtomicCopy.Transit(sql("LOAD INTO atomic TRANSIT MOCK")),
        List(sql("LOAD INTO SHRED 1 MOCK"), sql("LOAD INTO SHRED 2 MOCK"), sql("LOAD INTO SHRED 3 MOCK")),
        Some(List(sql("VACUUM MOCK"))),
        Some(List(sql("ANALYZE MOCK")))
      )

      val (state, result) = RedshiftLoader.loadFolder[Pure](input).run

      val expected = List(
        "CREATE TABLE schema.temp_transit_events ( LIKE schema.events )",
        "LOAD INTO atomic TRANSIT MOCK",
        "DROP TABLE schema.temp_transit_events",
        "LOAD INTO SHRED 1 MOCK",
        "LOAD INTO SHRED 2 MOCK",
        "LOAD INTO SHRED 3 MOCK",
      )

      val transactionsExpectation = state.getLog must beEqualTo(expected)
      val resultExpectation = result must beRight
      transactionsExpectation.and(resultExpectation)
    }
  }

  "run" should {
    "perform insertions, VACUUM and ANALYZE" >> {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(PureLogging.init.copy(print = PureLogging.noop))
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)

      val shreddedTypes = List(
        ShreddedType.Json(ShreddedType.Info(S3.Folder.coerce("s3://my-bucket/my-path"), "com.acme", "event", 2, Semver(1,5,0)), S3.Key.coerce("s3://assets/event_1.json")),
        ShreddedType.Json(ShreddedType.Info(S3.Folder.coerce("s3://my-bucket/my-path"), "com.acme", "context", 2, Semver(1,5,0)), S3.Key.coerce("s3://assets/context_2.json")),
        ShreddedType.Tabular(ShreddedType.Info(S3.Folder.coerce("s3://my-bucket/my-path"), "com.acme", "context", 3, Semver(1,5,0))),
      )
      val discovery = DataDiscovery(S3.Folder.coerce("s3://my-bucket/my-path"), shreddedTypes, Compression.Gzip)

      val (state, result) = RedshiftLoader.run[Pure](SpecHelpers.validConfig.copy(steps = Set(Step.Vacuum, Step.Analyze)), discovery).flatMap(identity).run

      val expected = List(
        "COPY atomic.events FROM 's3://my-bucket/my-path/kind=good/vendor=com.snowplowanalytics.snowplow/name=atomic/format=tsv/model=1/' CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789876:role/RedshiftLoadRole' REGION AS 'us-east-1' MAXERROR 1 TIMEFORMAT 'auto' DELIMITER ' ' EMPTYASNULL FILLRECORD TRUNCATECOLUMNS ACCEPTINVCHARS GZIP",
        "COPY atomic.com_acme_event_2 FROM 's3://my-bucket/my-path/kind=good/vendor=com.acme/name=event/format=json/model=2' CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789876:role/RedshiftLoadRole' JSON AS 's3://assets/event_1.json' REGION AS 'us-east-1' MAXERROR 1 TIMEFORMAT 'auto' TRUNCATECOLUMNS ACCEPTINVCHARS GZIP",
        "COPY atomic.com_acme_context_2 FROM 's3://my-bucket/my-path/kind=good/vendor=com.acme/name=context/format=json/model=2' CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789876:role/RedshiftLoadRole' JSON AS 's3://assets/context_2.json' REGION AS 'us-east-1' MAXERROR 1 TIMEFORMAT 'auto' TRUNCATECOLUMNS ACCEPTINVCHARS GZIP",
        "COPY atomic.com_acme_context_3 FROM 's3://my-bucket/my-path/kind=good/vendor=com.acme/name=context/format=tsv/model=3' CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789876:role/RedshiftLoadRole' REGION AS 'us-east-1' MAXERROR 1 TIMEFORMAT 'auto' DELIMITER ' ' TRUNCATECOLUMNS ACCEPTINVCHARS GZIP",
        "VACUUM SORT ONLY atomic.events",
        "VACUUM SORT ONLY atomic.com_acme_event_2",
        "VACUUM SORT ONLY atomic.com_acme_context_2",
        "VACUUM SORT ONLY atomic.com_acme_context_3",
        "BEGIN",
        "ANALYZE atomic.events",
        "ANALYZE atomic.com_acme_event_2",
        "ANALYZE atomic.com_acme_context_2",
        "ANALYZE atomic.com_acme_context_3",
        "COMMIT"
      )

      val transactionsExpectation = state.getLog must beEqualTo(expected)
      val resultExpectation = result must beRight
      transactionsExpectation.and(resultExpectation)
    }
  }
}
