package com.snowplowanalytics.snowplow.loader.snowflake.db

import cats.{Functor, Monad}
import cats.implicits._

import com.snowplowanalytics.snowplow.loader.snowflake.db.Statement.GetColumns.ShowColumnRow

/** Set of common functions to control DB entities */
object Control {
  def renameTable[C[_]: Functor: SfDao](schema: String, from: String, to: String): C[Unit] = ???

  def tableExists[C[_]: SfDao](dbSchema: String, tableName: String): C[Boolean] =
    SfDao[C].executeQuery[Boolean](Statement.TableExists(dbSchema, tableName))

  /** List all columns in the table */
  def getColumns[C[_]: Monad: SfDao](dbSchema: String, tableName: String): C[List[String]] =
    SfDao[C].executeQueryList[ShowColumnRow](Statement.GetColumns(dbSchema, tableName))
      .map(_.map(_.columnName))
}
