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
import com.snowplowanalytics.snowplow.loader.redshift.db.{RedshiftManifest, RsDao, Statement}
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry
import com.snowplowanalytics.snowplow.rdbloader.test.{Pure, PureDAO, TestState}
import org.specs2.mutable.Specification

class RedshiftManifestSpec extends Specification {

  "setup" should {
    "do nothing if table already exists and columns match" in {
      def getResult(s: TestState)(statement: Statement): Any =
        statement match {
          case Statement.TableExists("public", "manifest") => true
          case Statement.GetColumns("manifest")            => RedshiftManifest.Columns.map(_.columnName)
          case _                                           => throw new IllegalArgumentException(s"Unexpected query $statement with ${s.getLog}")
        }

      implicit val dao: RsDao[Pure] = PureDAO.interpreter(PureDAO.custom(getResult))
      lazy val manifest             = new RedshiftManifest[Pure]("public")
      val expected = List(
        LogEntry.Message(Statement.TableExists("public", "manifest").toFragment.toString()),
        LogEntry.Message(Statement.SetSchema("public").toFragment.toString()),
        LogEntry.Message(Statement.GetColumns("manifest").toFragment.toString())
      )

      val (state, value) = manifest.initialize.value.run(TestState.init).value
      (state.getLog must beEqualTo(expected)).and(value must beRight)
    }

    "rename legacy table and create new if legacy already exists (even with different column order)" in {
      def getResult(s: TestState)(statement: Statement): Any =
        statement match {
          case Statement.TableExists("public", "manifest") => true
          case Statement.GetColumns("manifest") =>
            List("commit_tstamp", "etl_tstamp", "shredded_cardinality", "event_count")
          case _ => throw new IllegalArgumentException(s"Unexpected query $statement with ${s.getLog}")
        }

      implicit val dao: RsDao[Pure] = PureDAO.interpreter(PureDAO.custom(getResult))
      lazy val manifest             = new RedshiftManifest[Pure]("public")
      val expected = List(
        LogEntry.Message(Statement.TableExists("public", "manifest").toFragment.toString()),
        LogEntry.Message(Statement.SetSchema("public").toFragment.toString()),
        LogEntry.Message(Statement.GetColumns("manifest").toFragment.toString()),
        LogEntry.Message(
          Statement
            .DdlFile(DdlFile(List(AlterTable("public.manifest", RenameTo("manifest_legacy")))))
            .toFragment
            .toString()
        ),
        LogEntry.Message(
          Statement
            .CreateTable(
              CreateTable(
                "public.manifest",
                RedshiftManifest.Columns,
                Set.empty,
                Set(Diststyle(Key), DistKeyTable("base"), SortKeyTable(None, NonEmptyList.one("ingestion_tstamp")))
              )
            )
            .toFragment
            .toString()
        ),
        LogEntry.Message(
          "The new manifest table has been created, legacy 0.1.0 manifest can be found at manifest_legacy and can be deleted manually"
        ),
        LogEntry.Message(Statement.CommentOn(CommentOn("public.manifest", "0.2.0")).toFragment.toString())
      )

      val (state, value) = manifest.initialize.value.run(TestState.init).value
      (state.getLog must beEqualTo(expected)).and(value must beRight)
    }

    "create table and add comment if it didn't exist" in {
      implicit val dao: RsDao[Pure] = PureDAO.interpreter(PureDAO.init)

      lazy val manifest = new RedshiftManifest[Pure]("public")
      val expected = List(
        LogEntry.Message(Statement.TableExists("public", "manifest").toFragment.toString()),
        LogEntry.Message(
          Statement
            .CreateTable(
              CreateTable(
                "public.manifest",
                RedshiftManifest.Columns,
                Set.empty,
                Set(Diststyle(Key), DistKeyTable("base"), SortKeyTable(None, NonEmptyList.one("ingestion_tstamp")))
              )
            )
            .toFragment
            .toString()
        ),
        LogEntry.Message("The manifest table has been created"),
        LogEntry.Message(Statement.CommentOn(CommentOn("public.manifest", "0.2.0")).toFragment.toString())
      )

      val (state, value) = manifest.initialize.value.run(TestState.init).value
      (state.getLog must beEqualTo(expected)).and(value must beRight)
    }
  }
}
