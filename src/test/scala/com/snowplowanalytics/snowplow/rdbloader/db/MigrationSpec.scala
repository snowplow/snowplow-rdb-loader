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

import cats.~>

import com.snowplowanalytics.snowplow.rdbloader.{LoaderA, S3}
import com.snowplowanalytics.snowplow.rdbloader.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}

import org.specs2.Specification

import com.snowplowanalytics.snowplow.rdbloader.TestInterpreter

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
    val input = List(DataDiscovery(S3.Folder.coerce("s3://shredded/archive"), None, None, types, true, None))

    val expected = List(
      "Fetch iglu:com.acme/some_context/jsonschema/2-0-0",
      "SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'com_acme_some_context_2') AS exists;",
      "CREATE TABLE IF NOT EXISTS public.com_acme_some_context_2 ( \"schema_vendor\" VARCHAR(128) ENCODE ZSTD NOT NULL, \"schema_name\" VARCHAR(128) ENCODE ZSTD NOT NULL, \"schema_format\" VARCHAR(128) ENCODE ZSTD NOT NULL, \"schema_version\" VARCHAR(128) ENCODE ZSTD NOT NULL, \"root_id\" CHAR(36) ENCODE RAW NOT NULL, \"root_tstamp\" TIMESTAMP ENCODE ZSTD NOT NULL, \"ref_root\" VARCHAR(255) ENCODE ZSTD NOT NULL, \"ref_tree\" VARCHAR(1500) ENCODE ZSTD NOT NULL, \"ref_parent\" VARCHAR(255) ENCODE ZSTD NOT NULL, \"\" VARCHAR(4096) ENCODE ZSTD NOT NULL, FOREIGN KEY (root_id) REFERENCES public.events(event_id) ) DISTSTYLE KEY DISTKEY (root_id) SORTKEY (root_tstamp)",
      "COMMENT ON TABLE public.com_acme_some_context_2 IS 'iglu:com.acme/some_context/jsonschema/2-0-0'"
    )

    val action = Migration.perform("public")(input)
    val (state, result) = action.value.foldMap(MigrationSpec.interpreter).run(Nil).value
    (state.reverse must beEqualTo(expected)).and(result must beRight)
  }
}

object MigrationSpec {

  import TestInterpreter.Test

  def interpreter: LoaderA ~> Test = new (LoaderA ~> Test) {
    def apply[A](effect: LoaderA[A]): Test[A] = {
      effect match {
        case LoaderA.ExecuteUpdate(query) =>
          TestInterpreter.executeUpdate(query)

        case LoaderA.ExecuteQuery(query, decoder) =>
          TestInterpreter.executeQuery(query, decoder)

        case LoaderA.GetSchemas(vendor, name, model) =>
          TestInterpreter.getSchemas(vendor, name, model)

        case action =>
          throw new RuntimeException(s"Unexpected Action [$action]")
      }
    }
  }

}