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
package com.snowplowanalytics.snowplow.loader.redshift.db

import doobie.Fragment

import com.snowplowanalytics.iglu.schemaddl.redshift._

import com.snowplowanalytics.snowplow.loader.redshift.Redshift.ManifestColumns

import com.snowplowanalytics.snowplow.rdbloader.db.{Statement, Manifest}
import com.snowplowanalytics.snowplow.rdbloader.dsl.DAO

import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.rdbloader.test.{PureDAO, Pure, TestState}
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry

class ManifestSpec extends Specification {
  "setup" should {
    "do nothing if table already exists and columns match" in {
      def getResult(s: TestState)(statement: Statement): Any =
        statement match {
          case Statement.TableExists("manifest") => true
          case Statement.GetColumns("manifest") => ManifestColumns.map(_.columnName)
          case _ => throw new IllegalArgumentException(s"Unexpected query $statement with ${s.getLog}")
        }

      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.custom(getResult))

      val expected = List(
        LogEntry.Sql(Statement.TableExists("manifest")),
        LogEntry.Sql(Statement.SetSchema),
        LogEntry.Sql(Statement.GetColumns("manifest")),
      )

      val (state, value) = Manifest.setup[Pure]("public").value.run(TestState.init).value
      (state.getLog must beEqualTo(expected)).and(value must beRight)
    }

    "rename legacy table and create new if legacy already exists (even with different column order)" in {
      def getResult(s: TestState)(statement: Statement): Any =
        statement match {
          case Statement.TableExists("manifest") => true
          case Statement.GetColumns("manifest") => List("commit_tstamp", "etl_tstamp", "shredded_cardinality", "event_count")
          case _ => throw new IllegalArgumentException(s"Unexpected query $statement with ${s.getLog}")
        }

      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.custom(getResult))

      val expected = List(
        LogEntry.Sql(Statement.TableExists("manifest")),
        LogEntry.Sql(Statement.SetSchema),
        LogEntry.Sql(Statement.GetColumns("manifest")),
        LogEntry.Sql(Statement.RenameTable("manifest", "manifest_legacy")),
        LogEntry.Sql(Statement.CreateTable(Fragment.const0("CREATE manifest"))),   // DDL comes from DummyTarget
        LogEntry.Sql(Statement.CommentOn("public.manifest","0.2.0"))
      )

      val (state, value) = Manifest.setup[Pure]("public").value.run(TestState.init).value
      (state.getLog must beEqualTo(expected)).and(value must beRight)
    }

    "create table and add comment if it didn't exist" in {
      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.init)

      val expected = List(
        LogEntry.Sql(Statement.TableExists("manifest")),
        LogEntry.Sql(Statement.CreateTable(Fragment.const0("CREATE manifest"))),   // DDL comes from DummyTarget
        LogEntry.Sql(Statement.CommentOn("public.manifest","0.2.0"))
      )

      val (state, value) = Manifest.setup[Pure]("public").value.run(TestState.init).value
      state.getLog must beEqualTo(expected)
      value must beRight
    }
  }
}
