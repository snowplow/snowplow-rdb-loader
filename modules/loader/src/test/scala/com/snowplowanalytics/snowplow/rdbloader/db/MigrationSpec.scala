/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.db

import com.snowplowanalytics.snowplow.rdbloader.TestInterpreter
import com.snowplowanalytics.snowplow.rdbloader.TestInterpreter.{ControlResults, JDBCResults, Test, TestState}
import com.snowplowanalytics.snowplow.rdbloader.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Iglu, JDBC, Logging}
import com.snowplowanalytics.snowplow.rdbloader.utils.S3
import org.specs2.Specification

class MigrationSpec extends Specification { def is = s2"""
  Perform migration only for ShreddedType.Tabular $e1
  """

  def e1 = {
    val types =
      List(
        ShreddedType.Tabular(ShreddedType.Info(
          S3.Folder.coerce("s3://shredded/archive"),
          "com.acme",
          "some_context",
          2,
          Semver(0, 17, 0)
        )),
        ShreddedType.Json(ShreddedType.Info(
          S3.Folder.coerce("s3://shredded/archive"),
          "com.acme",
          "some_event",
          1,
          Semver(0, 17, 0)
        ), S3.Key.coerce("s3://shredded/jsonpaths"))
      )
    val input = List(DataDiscovery(S3.Folder.coerce("s3://shredded/archive"), None, None, types, true))

    val expected = List(
      "Fetch iglu:com.acme/some_context/jsonschema/2-0-0",
      "SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'com_acme_some_context_2') AS exists;",
      "Creating public.com_acme_some_context_2 table for iglu:com.acme/some_context/jsonschema/2-0-0",
      "CREATE TABLE IF NOT EXISTS public.com_acme_some_context_2 ( \"schema_vendor\" VARCHAR(128) ENCODE ZSTD NOT NULL, \"schema_name\" VARCHAR(128) ENCODE ZSTD NOT NULL, \"schema_format\" VARCHAR(128) ENCODE ZSTD NOT NULL, \"schema_version\" VARCHAR(128) ENCODE ZSTD NOT NULL, \"root_id\" CHAR(36) ENCODE RAW NOT NULL, \"root_tstamp\" TIMESTAMP ENCODE ZSTD NOT NULL, \"ref_root\" VARCHAR(255) ENCODE ZSTD NOT NULL, \"ref_tree\" VARCHAR(1500) ENCODE ZSTD NOT NULL, \"ref_parent\" VARCHAR(255) ENCODE ZSTD NOT NULL, FOREIGN KEY (root_id) REFERENCES public.events(event_id) ) DISTSTYLE KEY DISTKEY (root_id) SORTKEY (root_tstamp)",
      "COMMENT ON TABLE public.com_acme_some_context_2 IS 'iglu:com.acme/some_context/jsonschema/2-0-0'",
      "Table created"
    )

    implicit val jdbc: JDBC[Test] = TestInterpreter.stateJdbcInterpreter(JDBCResults.init)
    implicit val iglu: Iglu[Test] = TestInterpreter.stateIgluInterpreter
    implicit val control: Logging[Test] = TestInterpreter.stateControlInterpreter(ControlResults.init)

    val (state, result) = Migration.perform[Test]("public")(input).value.run(TestState.init).value
    (state.getLog must beEqualTo(expected)).and(result must beRight)
  }
}
