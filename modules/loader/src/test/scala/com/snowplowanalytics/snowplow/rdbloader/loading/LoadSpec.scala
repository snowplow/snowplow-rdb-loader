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
package com.snowplowanalytics.snowplow.rdbloader.loading

import cats.effect.Timer
import cats.syntax.either._
import cats.syntax.alternative._

import com.snowplowanalytics.snowplow.rdbloader.common.Config.Compression
import com.snowplowanalytics.snowplow.rdbloader.{LoaderError, SpecHelpers, LoaderAction}
import com.snowplowanalytics.snowplow.rdbloader.common.{S3, Message, Semver}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, Iglu, JDBC}
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.SqlString
import com.snowplowanalytics.snowplow.rdbloader.loading.LoadSpec.{failCommit, isFirstCommit}

import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.rdbloader.test.{Pure, TestState, PureIglu, PureJDBC, PureOps, PureLogging, PureTimer}

class LoadSpec extends Specification {
  "load" should {
    "perform COPY statements and wrap with transaction block" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(PureLogging.noPrint)
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val timer: Timer[Pure] = PureTimer.interpreter

      val message = Message(LoadSpec.dataDiscovery, Pure.pure(()))

      val expected = List(
        "BEGIN",
        "COPY atomic.events FROM 's3://shredded/base/vendor=com.snowplowanalytics.snowplow/name=atomic/format=tsv/model=1/' CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789876:role/RedshiftLoadRole' REGION AS 'us-east-1' MAXERROR 1 TIMEFORMAT 'auto' DELIMITER ' ' EMPTYASNULL FILLRECORD TRUNCATECOLUMNS ACCEPTINVCHARS GZIP",
        "COPY atomic.com_acme_json_context_1 FROM 's3://shredded/base/vendor=com.acme/name=json-context/format=json/model=1' CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789876:role/RedshiftLoadRole' JSON AS 's3://assets/com.acme/json_context_1.json' REGION AS 'us-east-1' MAXERROR 1 TIMEFORMAT 'auto' TRUNCATECOLUMNS ACCEPTINVCHARS GZIP",
        "COMMIT"
      )

      val result = Load.load[Pure](SpecHelpers.validCliConfig, message).runS

      result.getLog must beEqualTo(expected)
    }

    "perform ack before COMMIT" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(PureLogging.noPrint)
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val timer: Timer[Pure] = PureTimer.interpreter

      val message = Message(LoadSpec.dataDiscovery, Pure.modify(_.log("ACK")))

      val expected = List(
        "BEGIN",
        "COPY atomic.events FROM 's3://shredded/base/vendor=com.snowplowanalytics.snowplow/name=atomic/format=tsv/model=1/' CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789876:role/RedshiftLoadRole' REGION AS 'us-east-1' MAXERROR 1 TIMEFORMAT 'auto' DELIMITER ' ' EMPTYASNULL FILLRECORD TRUNCATECOLUMNS ACCEPTINVCHARS GZIP",
        "COPY atomic.com_acme_json_context_1 FROM 's3://shredded/base/vendor=com.acme/name=json-context/format=json/model=1' CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789876:role/RedshiftLoadRole' JSON AS 's3://assets/com.acme/json_context_1.json' REGION AS 'us-east-1' MAXERROR 1 TIMEFORMAT 'auto' TRUNCATECOLUMNS ACCEPTINVCHARS GZIP",
        "ACK",
        "COMMIT"
      )

      val result = Load.load[Pure](SpecHelpers.validCliConfig, message).runS

      result.getLog must beEqualTo(expected)
    }

    "not perform COMMIT if ack failed with RuntimeException" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(PureLogging.noPrint)
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val timer: Timer[Pure] = PureTimer.interpreter

      val message = Message(LoadSpec.dataDiscovery, Pure.fail[Unit](new RuntimeException("Failed ack")))

      val expected = List(
        "BEGIN",
        "COPY atomic.events FROM 's3://shredded/base/vendor=com.snowplowanalytics.snowplow/name=atomic/format=tsv/model=1/' CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789876:role/RedshiftLoadRole' REGION AS 'us-east-1' MAXERROR 1 TIMEFORMAT 'auto' DELIMITER ' ' EMPTYASNULL FILLRECORD TRUNCATECOLUMNS ACCEPTINVCHARS GZIP",
        "COPY atomic.com_acme_json_context_1 FROM 's3://shredded/base/vendor=com.acme/name=json-context/format=json/model=1' CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789876:role/RedshiftLoadRole' JSON AS 's3://assets/com.acme/json_context_1.json' REGION AS 'us-east-1' MAXERROR 1 TIMEFORMAT 'auto' TRUNCATECOLUMNS ACCEPTINVCHARS GZIP",
      )

      val result = Load.load[Pure](SpecHelpers.validCliConfig, message).runS

      result.getLog must beEqualTo(expected)
    }

    "abort, sleep and start transaction again if first commit failed" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(PureLogging.noPrint)
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init.withExecuteUpdate(isFirstCommit, failCommit))
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val timer: Timer[Pure] = PureTimer.interpreter

      val message = Message(LoadSpec.dataDiscovery, Pure.pure(()))

      val expected = List("BEGIN", "COPY", "COPY", "ABORT", "SLEEP", "BEGIN", "COPY", "COPY", "COMMIT")

      val result = Load.load[Pure](SpecHelpers.validCliConfig, message).runS

      result.getLog.map(_.split(" ").headOption).unite must beEqualTo(expected)
    }
  }
}

object LoadSpec {
  val dataDiscovery = DataDiscovery(
    S3.Folder.coerce("s3://shredded/base/"),
    List(
      ShreddedType.Json(
        ShreddedType.Info(
          S3.Folder.coerce("s3://shredded/base/"),
          "com.acme", "json-context", 1, Semver(0,18,0, None)
        ),
        S3.Key.coerce("s3://assets/com.acme/json_context_1.json"),
      )
    ),
    Compression.Gzip
  )

  def isFirstCommit(sql: SqlString, ts: TestState) =
    sql.contains("COMMIT") && ts.getLog.length < 4
  val failCommit: LoaderAction[Pure, Long] =
    LoaderAction.liftE[Pure, Long](LoaderError.StorageTargetError("Commit failed").asLeft)
}