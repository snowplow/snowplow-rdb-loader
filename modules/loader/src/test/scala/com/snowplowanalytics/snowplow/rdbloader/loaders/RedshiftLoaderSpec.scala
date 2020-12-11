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

import org.specs2.Specification

// This project
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder

import com.snowplowanalytics.snowplow.rdbloader.TestInterpreter
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, JDBC}
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.loaders.Common.SqlString.{unsafeCoerce => sql}

import com.snowplowanalytics.snowplow.rdbloader.TestInterpreter.{JDBCResults, ControlResults, TestState, Test}

class RedshiftLoaderSpec extends Specification { def is = s2"""
  Perform manifest-insertion and load within same transaction $e5
  """

  val noDiscovery = DataDiscovery(Folder.coerce("s3://noop"), Nil)

  def e5 = {
    implicit val control: Logging[Test] = TestInterpreter.stateControlInterpreter(ControlResults.init.copy(print = ControlResults.noop))
    implicit val jdbc: JDBC[Test] = TestInterpreter.stateJdbcInterpreter(JDBCResults.init)

    val input = RedshiftLoadStatements(
      "atomic",
      RedshiftLoadStatements.AtomicCopy.Straight(sql("LOAD INTO atomic MOCK")),
      List(sql("LOAD INTO SHRED 1 MOCK"), sql("LOAD INTO SHRED 2 MOCK"), sql("LOAD INTO SHRED 3 MOCK")),
      Some(List(sql("VACUUM MOCK"))),   // Must be shred cardinality + 1
      Some(List(sql("ANALYZE MOCK"))),
      noDiscovery
    )

    val (state, result) = RedshiftLoader.loadFolder[Test](input).value.run(TestState.init).value

    val expected = List(
      "BEGIN",
      "LOAD INTO atomic MOCK",
      "LOAD INTO SHRED 1 MOCK",
      "LOAD INTO SHRED 2 MOCK",
      "LOAD INTO SHRED 3 MOCK",
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
}

