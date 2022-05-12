/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import doobie.{Read, ConnectionIO}
import doobie.free.connection

import com.snowplowanalytics.snowplow.rdbloader.db.{ Statement, Target }

/**
 * An effect declaration of communicating with a DB.
 * Typically, it represents a second effect from the main app, i.e.
 * communication with DB happens within `C` effect and then it's
 * translated with [[Transaction]] into `F`.
 *
 * This is done mostly to mimic doobie's `ConnectionIO` behavior,
 * which is is a separate effect (not `IO`). Also we cannot have
 * `ConnectionIO` anywhere in tests as it's impossible to inspect
 */
trait DAO[C[_]] {

  /** Execute single SQL statement */
  def executeUpdate(sql: Statement): C[Int]

  /** Execute query and parse results into `A` */
  def executeQuery[A](query: Statement)(implicit A: Read[A]): C[A]

  /** Execute query and parse results into 0 or more `A`s */
  def executeQueryList[A](query: Statement)(implicit A: Read[A]): C[List[A]]

  /** Execute query and parse results into 0 or one `A` */
  def executeQueryOption[A](query: Statement)(implicit A: Read[A]): C[Option[A]]

  /** Rollback the transaction */
  def rollback: C[Unit]

  /** Get the DB interpreter */
  def target: Target
}

object DAO {

  def apply[F[_]](implicit ev: DAO[F]): DAO[F] = ev

  def connectionIO(dbTarget: Target): DAO[ConnectionIO] = new DAO[ConnectionIO] {
    /** Execute single SQL statement (against target in interpreter) */
    def executeUpdate(sql: Statement): ConnectionIO[Int] =
      dbTarget.toFragment(sql).update.run

    /** Execute query and parse results into `A` */
    def executeQuery[A](query: Statement)(implicit A: Read[A]): ConnectionIO[A] =
      dbTarget.toFragment(query).query[A].unique

    def executeQueryList[A](query: Statement)(implicit A: Read[A]): ConnectionIO[List[A]] =
      dbTarget.toFragment(query).query[A].to[List]

    def executeQueryOption[A](query: Statement)(implicit A: Read[A]): ConnectionIO[Option[A]] =
      dbTarget.toFragment(query).query[A].option

    def rollback: ConnectionIO[Unit] =
      connection.rollback

    def target: Target =
      dbTarget
  }
}