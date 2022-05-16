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

import cats.data.NonEmptyList
import cats.implicits._

import doobie.{Read, Fragment}

import com.snowplowanalytics.iglu.core.{SchemaVer, SchemaKey}

import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.migrations.{FlatSchema, SchemaList, Migration => SchemaMigration}
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.DdlGenerator

import com.snowplowanalytics.snowplow.rdbloader.{LoaderError, LoadStatements}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.db.Migration.{Item, Block}
import com.snowplowanalytics.snowplow.rdbloader.db.{Target, Statement, Migration}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
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
      case Statement.GetVersion(_) => SchemaKey("com.acme", "some_context", "jsonschema", SchemaVer.Full(2,0,0))
      case Statement.TableExists(_) => false
      case Statement.GetColumns(_) => List("some_column")
      case Statement.ManifestGet(_) => None
      case Statement.FoldersMinusManifest => List()
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

  def interpreter(results: PureDAO, tgt: Target = DummyTarget): DAO[Pure] = new DAO[Pure] {
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

    def target: Target = tgt
  }

  val DummyTarget = new Target {
    def toFragment(statement: Statement): Fragment =
      Fragment.const0(statement.toString)

    def getLoadStatements(discovery: DataDiscovery): LoadStatements =
      NonEmptyList(
        Statement.EventsCopy(discovery.base, Compression.Gzip, List.empty),
        discovery.shreddedTypes.map { shredded =>
          Statement.ShreddedCopy(shredded, Compression.Gzip)
        }
      )

    def getManifest: Statement =
      Statement.CreateTable(Fragment.const0("CREATE manifest"))

    def updateTable(migration: SchemaMigration): Migration.Block =
      throw LoaderError.RuntimeError("Not implemented in test suite")

    def extendTable(info: ShreddedType.Info): Block =
      throw LoaderError.RuntimeError("Not implemented in test suite")

    def createTable(schemas: SchemaList): Migration.Block = {
      val subschemas = FlatSchema.extractProperties(schemas)
      val tableName = StringUtils.getTableName(schemas.latest)
      val createTable = DdlGenerator.generateTableDdl(subschemas, tableName, Some("public"), 4096, false)
      val entity = Migration.Entity.Table("public", schemas.latest.schemaKey)
      Block(Nil, List(Item.CreateTable(Fragment.const0(createTable.toDdl))), entity)
    }
  }
}
