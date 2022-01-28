package com.snowplowanalytics.snowplow.loader.snowflake.db

import cats.syntax.all._
import cats.data.NonEmptyList

import com.snowplowanalytics.iglu.core.SchemaVer.Full
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.IgluSchema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties._
import com.snowplowanalytics.iglu.schemaddl.migrations.{SchemaList => DSchemaList}
import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.test.{Pure, TestState}
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry
import com.snowplowanalytics.snowplow.rdbloader.discovery.ShreddedType
import com.snowplowanalytics.snowplow.rdbloader.common.{S3, LoaderMessage}
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.MigrationBuilder
import com.snowplowanalytics.snowplow.loader.snowflake.db.ast.{AlterTable, SnowflakeDatatype}
import com.snowplowanalytics.snowplow.loader.snowflake.test.PureDAO

import org.specs2.mutable.Specification

class SnowflakeMigrationSpec extends Specification {
  import SnowflakeMigrationSpec._

  "build" should {
    "add new column when column with same name does not exist" in {
      implicit val dao: SfDao[Pure] = PureDAO.interpreter(PureDAO.init)
      lazy val migration = new SnowflakeMigrationBuilder[Pure](dbSchema)

      val input = List(MigrationBuilder.MigrationItem(shreddedType, schemaListTwo))

      val expected = List(
        LogEntry.Message(
          Statement.GetColumns(dbSchema, tableName).toFragment.toString
        )
      )
      val expectedMigration = List(
        LogEntry.Message(
          "Creating new column for schema key iglu:com.acme/some_context/jsonschema/1-0-1"
        ),
        LogEntry.Message(
          AlterTable.AddColumn(
            dbSchema,
            tableName,
            "unstruct_event_com_acme_some_context_1",
            SnowflakeDatatype.JsonObject
          ).toStatement.toFragment.toString
        ),
        LogEntry.Message(
          "New column is created for schema key iglu:com.acme/some_context/jsonschema/1-0-1"
        )
      )

      val (state, value) = migration.build(input).run

      state.getLog must beEqualTo(expected)
      value.rethrow must beRight.like {
        case MigrationBuilder.Migration(pre, in) =>
          pre.runS.getLog must beEqualTo(expectedMigration)
          in.runS.getLog must beEmpty
      }
    }

    "not add new column when column with same name exists" in {
      def getResult(s: TestState)(query: Statement): Any =
        query match {
          case Statement.GetColumns(`dbSchema`, `tableName`) => List("unstruct_event_com_acme_some_context_1")
          case statement => PureDAO.getResult(s)(statement)
        }
      implicit val dao: SfDao[Pure] = PureDAO.interpreter(PureDAO.custom(getResult))
      lazy val migration = new SnowflakeMigrationBuilder[Pure](dbSchema)

      val input = List(MigrationBuilder.MigrationItem(shreddedType, schemaListTwo))

      val expected = List(
        LogEntry.Message(
          Statement.GetColumns(dbSchema, tableName).toFragment.toString
        )
      )

      val (state, value) = migration.build(input).run

      state.getLog must beEqualTo(expected)
      value.rethrow must beRight.like {
        case MigrationBuilder.Migration(pre, in) =>
          pre.runS.getLog must beEmpty
          in.runS.getLog must beEmpty
      }
    }

    "throw error when given shred type is not supported" in {
      implicit val dao: SfDao[Pure] = PureDAO.interpreter(PureDAO.init)
      lazy val migration = new SnowflakeMigrationBuilder[Pure](dbSchema)

      val shreddedType = ShreddedType.Tabular(
        ShreddedType.Info(
          s3Folder,
          schemaKey.vendor,
          schemaKey.name,
          schemaKey.version.model,
          Semver(0, 17, 0),
          LoaderMessage.ShreddedType.SelfDescribingEvent
        )
      )

      val input = List(MigrationBuilder.MigrationItem(shreddedType, schemaListTwo))

      val (state, value) = migration.build(input).run

      state.getLog must beEmpty
      value.rethrow must beLeft.like {
        case LoaderError.MigrationError(_) => ok
      }
    }
  }
}

object SnowflakeMigrationSpec {
  val dbSchema = "public"
  val tableName = SnowflakeMigrationBuilder.EventTable
  val schemaKey = SchemaKey("com.acme", "some_context", "jsonschema", Full(1, 0, 0))
  val schema100 = SelfDescribingSchema(
    SchemaMap(schemaKey),
    Schema(
      properties = Some(
        ObjectProperty.Properties(
          Map(
            "one" -> Schema(),
            "two" -> Schema()
          )
        )
      )
    )
  )
  val schema101 = SelfDescribingSchema(
    SchemaMap(schemaKey.copy(version = Full(1, 0, 1))),
    Schema(
      properties = Some(
        ObjectProperty.Properties(
          Map(
            "one"   -> Schema(),
            "two"   -> Schema(),
            "three" -> Schema()
          )
        )
      )
    )
  )
  val schemaListTwo = createSchemaList(
    NonEmptyList.of(
      SnowflakeMigrationSpec.schema100,
      SnowflakeMigrationSpec.schema101
    )
  )

  val s3Folder = S3.Folder.coerce("s3://shredded/archive")
  val shreddedType = createShreddedType(schemaKey.copy(version = Full(1, 0, 1)))

  def createSchemaList(schemas: NonEmptyList[IgluSchema]): DSchemaList =
    DSchemaList
      .unsafeBuildWithReorder(
        DSchemaList.ModelGroupSet.groupSchemas(schemas).head
      ).getOrElse(throw new RuntimeException("Cannot create SchemaList"))

  def createShreddedType(schemaKey: SchemaKey,
                         s3Folder: S3.Folder = SnowflakeMigrationSpec.s3Folder,
                         shredProperty: LoaderMessage.ShreddedType.ShredProperty = LoaderMessage.ShreddedType.SelfDescribingEvent
                        ): ShreddedType =
    ShreddedType.Widerow(
      ShreddedType.Info(
        s3Folder,
        schemaKey.vendor,
        schemaKey.name,
        schemaKey.version.model,
        Semver(0, 17, 0),
        shredProperty
      )
    )
}
