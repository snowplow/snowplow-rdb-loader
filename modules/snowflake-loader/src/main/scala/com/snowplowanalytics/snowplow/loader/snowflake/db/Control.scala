package com.snowplowanalytics.snowplow.loader.snowflake.db

import cats.{Functor, Monad}
import cats.syntax.all._
import com.snowplowanalytics.iglu.schemaddl.redshift.{AlterTable, RenameTo}
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.DdlFile

/** Set of common functions to control DB entities */
object Control {
  def renameTable[C[_]: Functor: SfDao](schema: String, from: String, to: String): C[Unit] = ???

  def tableExists[C[_]: SfDao](dbSchema: String, tableName: String): C[Boolean] = ???

  /** List all columns in the table */
  def getColumns[C[_]: Monad: SfDao](dbSchema: String, tableName: String): C[List[String]] = ???
}
