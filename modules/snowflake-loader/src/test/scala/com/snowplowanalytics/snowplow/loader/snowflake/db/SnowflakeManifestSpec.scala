package com.snowplowanalytics.snowplow.loader.snowflake.db

import com.snowplowanalytics.snowplow.rdbloader.test.{Pure, TestState}
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry
import com.snowplowanalytics.snowplow.loader.snowflake.test._
import com.snowplowanalytics.snowplow.loader.snowflake.db.ast.CreateTable
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
      lazy val manifest = new SnowflakeManifest[Pure](dbSchema)

      val expected = List(
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
      lazy val manifest = new SnowflakeManifest[Pure](dbSchema)

      val expected = List(
        LogEntry.Message(Statement.TableExists(dbSchema, tableName).toTestString),
        LogEntry.Message(
          CreateTable(
            dbSchema,
            tableName,
            SnowflakeManifest.Columns,
            Some(SnowflakeManifest.ManifestPK)
          ).toStatement.toTestString
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
}
