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
package com.snowplowanalytics.snowplow.rdbloader.db

import cats.data.NonEmptyList

import com.snowplowanalytics.iglu.schemaddl.redshift._
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.DdlFile

import com.snowplowanalytics.snowplow.rdbloader.dsl.JDBC
import com.snowplowanalytics.snowplow.rdbloader.test.{PureJDBC, Pure, TestState}
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry

import org.specs2.mutable.Specification

class ManifestSpec extends Specification {
  "setup" should {
    "do nothing if table already exists and columns match" in {
      def getResult(s: TestState)(statement: Statement): Any =
        statement match {
          case Statement.TableExists("public", "manifest") => true
          case Statement.GetColumns("manifest") => Manifest.Columns.map(_.columnName)
          case _ => throw new IllegalArgumentException(s"Unexpected query $statement with ${s.getLog}")
        }

      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.custom(getResult))

      val expected = List(
        LogEntry.Sql(Statement.TableExists("public","manifest")),
        LogEntry.Sql(Statement.SetSchema("public")),
        LogEntry.Sql(Statement.GetColumns("manifest")),
      )

      val (state, value) = Manifest.setup[Pure]("public").run
      (state.getLog must beEqualTo(expected)).and(value must beRight)
    }

    "rename legacy table and create new if legacy already exists (even with different column order)" in {
      def getResult(s: TestState)(statement: Statement): Any =
        statement match {
          case Statement.TableExists("public", "manifest") => true
          case Statement.GetColumns("manifest") => List("commit_tstamp", "etl_tstamp", "shredded_cardinality", "event_count")
          case _ => throw new IllegalArgumentException(s"Unexpected query $statement with ${s.getLog}")
        }

      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.custom(getResult))

      val expected = List(
        LogEntry.Sql(Statement.TableExists("public","manifest")),
        LogEntry.Sql(Statement.SetSchema("public")),
        LogEntry.Sql(Statement.GetColumns("manifest")),
        LogEntry.Sql(Statement.DdlFile(DdlFile(List(AlterTable("public.manifest",RenameTo("manifest_legacy")))))),
        LogEntry.Sql(Statement.CreateTable(CreateTable(
          "public.manifest",
          Manifest.Columns,
          Set.empty,
          Set(Diststyle(Key), DistKeyTable("base"), SortKeyTable(None,NonEmptyList.one("ingestion_tstamp")))))),
        LogEntry.Sql(Statement.CommentOn(CommentOn("public.manifest","0.2.0")))
      )

      val (state, value) = Manifest.setup[Pure]("public").run
      (state.getLog must beEqualTo(expected)).and(value must beRight)
    }

    "create table and add comment if it didn't exist" in {
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)

      val expected = List(
        LogEntry.Sql(Statement.TableExists("public","manifest")),
        LogEntry.Sql(Statement.CreateTable(CreateTable(
          "public.manifest",
          Manifest.Columns,
          Set.empty,
          Set(Diststyle(Key), DistKeyTable("base"), SortKeyTable(None,NonEmptyList.one("ingestion_tstamp")))))),
        LogEntry.Sql(Statement.CommentOn(CommentOn("public.manifest","0.2.0")))
      )

      val (state, value) = Manifest.setup[Pure]("public").run
      (state.getLog must beEqualTo(expected)).and(value must beRight)
    }
  }
}
