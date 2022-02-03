/*
 * Copyright (c) 2022-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.loader.snowflake.db

import com.snowplowanalytics.snowplow.rdbloader.test.{Pure, TestState}
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry
import com.snowplowanalytics.snowplow.loader.snowflake.test._
import org.specs2.mutable.Specification

class SnowflakeManifestSpec extends Specification{
  import SnowflakeManifestSpec._

  "setup" should {
    "do nothing if table already exists" in {
      def getResult(s: TestState)(query: Statement): Any =
        query match {
          case Statement.TableExists(`dbSchema`, `tableName`) => true
          case statement => PureDAO.getResult(s)(statement)
        }
      implicit val dao: SfDao[Pure] = PureDAO.interpreter(PureDAO.custom(getResult))
      lazy val manifest = new SnowflakeManifest[Pure](dbSchema, warehouse)

      val expected = List(
        LogEntry.Message(Statement.WarehouseResume(warehouse).toTestString),
        LogEntry.Message(Statement.TableExists(dbSchema, tableName).toTestString)
      )

      val (state, value) = manifest.initialize.value.run(TestState.init).value

      state.getLog must beEqualTo(expected)
      value must beRight
    }

    "create table if it didn't exist" in {
      def getResult(s: TestState)(query: Statement): Any =
        query match {
          case Statement.TableExists(`dbSchema`, `tableName`) => false
          case statement => PureDAO.getResult(s)(statement)
        }
      implicit val dao: SfDao[Pure] = PureDAO.interpreter(PureDAO.custom(getResult))
      lazy val manifest = new SnowflakeManifest[Pure](dbSchema, warehouse)

      val expected = List(
        LogEntry.Message(Statement.WarehouseResume(warehouse).toTestString),
        LogEntry.Message(Statement.TableExists(dbSchema, tableName).toTestString),
        LogEntry.Message(
          Statement.CreateTable(
            dbSchema,
            tableName,
            SnowflakeManifest.Columns,
            Some(SnowflakeManifest.ManifestPK)
          ).toTestString
        ),
        LogEntry.Message("The manifest table has been created")
      )

      val (state, value) = manifest.initialize.value.run(TestState.init).value
      
      state.getLog must beEqualTo(expected)
      value must beRight
    }
  }

}

object SnowflakeManifestSpec {
  val dbSchema = "public"
  val tableName = SnowflakeManifest.ManifestTable
  val warehouse = "testwarehouse"
}
