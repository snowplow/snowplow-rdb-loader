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
package com.snowplowanalytics.snowplow.rdbloader.test

import cats.implicits._

import doobie.Read

import com.snowplowanalytics.iglu.core.{SchemaVer, SchemaKey}

import com.snowplowanalytics.snowplow.rdbloader.db.Statement
import com.snowplowanalytics.snowplow.rdbloader.dsl.DAO


case class PureDAO(executeQuery: Statement => Pure[Any],
                   executeUpdate: Statement => Pure[Int]) {

  /** If certain predicate met, return `update` action, otherwise do usual `executeUpdate` */
  def withExecuteUpdate(predicate: (Statement, TestState) => Boolean, update: Pure[Int]): PureDAO = {
    val updated = (sql: Statement) => {
      Pure { (ts: TestState) =>
        if (predicate(sql, ts))
          (ts, update)
        else (ts, executeUpdate(sql))
      }.flatten
    }
    this.copy(executeUpdate = updated)
  }
}

object PureDAO {

  def getResult(s: TestState)(query: Statement): Any =
    query match {
      case Statement.GetVersion(_, _) => SchemaKey("com.acme", "some_context", "jsonschema", SchemaVer.Full(2,0,0))
      case Statement.TableExists(_, _) => false
      case Statement.GetColumns(_) => List("some_column")
      case Statement.ManifestGet(_, _) => None
      case Statement.FoldersMinusManifest(_) => List()
      case _ => throw new IllegalArgumentException(s"Unexpected query $query with ${s.getLog}")
    }

  def custom(getResult: TestState => Statement => Any): PureDAO = {
    def executeQuery(query: Statement): Pure[Any] = {
      Pure((s: TestState) => (s.log(query), getResult(s)(query).asInstanceOf[Any]))
    }

    def executeUpdate(sql: Statement): Pure[Int] =
      Pure((s: TestState) => (s.log(sql), 1))

    PureDAO(q => executeQuery(q), executeUpdate)
  }

  val init: PureDAO = custom(getResult)

  def interpreter(results: PureDAO): DAO[Pure] = new DAO[Pure] {
    def executeUpdate(sql: Statement): Pure[Int] =
      results.executeUpdate(sql)

    def executeQuery[A](query: Statement)(implicit A: Read[A]): Pure[A] =
      results.executeQuery.asInstanceOf[Statement => Pure[A]](query)

    def executeQueryList[A](query: Statement)(implicit A: Read[A]): Pure[List[A]] =
      results.executeQuery.asInstanceOf[Statement => Pure[List[A]]](query)

    def executeQueryOption[A](query: Statement)(implicit A: Read[A]): Pure[Option[A]] =
      results.executeQuery.asInstanceOf[Statement => Pure[Option[A]]](query)

    def rollback: Pure[Unit] =
      Pure.modify(_.log(PureTransaction.Rollback))

  }

}
