package com.snowplowanalytics.snowplow.rdbloader.test.dao

import cats.syntax.all._
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList
import com.snowplowanalytics.snowplow.rdbloader.{LoaderAction, LoaderError}
import com.snowplowanalytics.snowplow.rdbloader.algebras.db.MigrationBuilder
import com.snowplowanalytics.snowplow.rdbloader.test.Pure

object PureMigrationBuilder {
  def interpreter: MigrationBuilder[Pure] = new MigrationBuilder[Pure] {
    override def build(
      items: List[MigrationBuilder.MigrationItem]
    ): LoaderAction[Pure, MigrationBuilder.Migration[Pure]] =
      Pure
        .sql(s"MigrationBuilder build")
        .as(
          MigrationBuilder
            .Migration[Pure](
              Pure.log(s"premigration ${items.map {
                case MigrationBuilder.MigrationItem(_, SchemaList.Full(schemas)) =>
                  schemas.map(_.self.schemaKey.toSchemaUri).toList.mkString(", ")
                case MigrationBuilder.MigrationItem(_, SchemaList.Single(schema)) => schema.self.schemaKey.toSchemaUri
              }}"),
              Pure.log("postmigration")
            )
            .asRight[LoaderError]
        )
        .toAction
  }
}
