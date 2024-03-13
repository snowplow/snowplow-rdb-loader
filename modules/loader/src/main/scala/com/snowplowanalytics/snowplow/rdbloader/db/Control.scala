/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
