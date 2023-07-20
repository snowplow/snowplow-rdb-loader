/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.db

import cats.{Functor, Monad}
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.db.Columns.ColumnName
import com.snowplowanalytics.snowplow.rdbloader.dsl.DAO

/** Set of common functions to control DB entities */
object Control {
  def renameTable[F[_]: Functor: DAO](from: String, to: String): F[Unit] =
    DAO[F].executeUpdate(Statement.RenameTable(from, to), DAO.Purpose.NonLoading).void

  def tableExists[F[_]: DAO](tableName: String): F[Boolean] =
    DAO[F].executeQuery[Boolean](Statement.TableExists(tableName))

  def getColumns[F[_]: Monad: DAO](tableName: String): F[List[ColumnName]] =
    DAO[F]
      .executeQueryList[String](Statement.GetColumns(tableName))
      .map(_.map(ColumnName))
}
