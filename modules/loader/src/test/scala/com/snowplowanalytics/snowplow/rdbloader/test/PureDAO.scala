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
package com.snowplowanalytics.snowplow.rdbloader.test

import cats.Monad
import cats.data.NonEmptyList
import cats.implicits._
import doobie.{Fragment, Read}
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer}
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModel
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import com.snowplowanalytics.snowplow.rdbloader.LoadStatements
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.db.Columns.{ColumnsToCopy, ColumnsToSkip, EventTableColumns}
import com.snowplowanalytics.snowplow.rdbloader.db.Migration.{Block, Item}
import com.snowplowanalytics.snowplow.rdbloader.db.{Migration, Statement, Target}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.DAO

case class PureDAO(executeQuery: Statement => Pure[Any], executeUpdate: Statement => Pure[Int]) {

  /** If certain predicate met, return `update` action, otherwise do usual `executeUpdate` */
  def withExecuteUpdate(predicate: (Statement, TestState) => Boolean, update: Pure[Int]): PureDAO = {
    val updated = (sql: Statement) =>
      Pure { (ts: TestState) =>
        if (predicate(sql, ts))
          (ts, update)
        else (ts, executeUpdate(sql))
      }.flatten
    this.copy(executeUpdate = updated)
  }
}

object PureDAO {

  def getResult(s: TestState)(query: Statement): Any =
    query match {
      case Statement.GetVersion(_) => SchemaKey("com.acme", "some_context", "jsonschema", SchemaVer.Full(2, 0, 0))
      case Statement.TableExists(_) => false
      case Statement.GetColumns(_) => List("some_column")
      case Statement.ManifestGet(_) => List()
      case Statement.FoldersMinusManifest => List()
      case Statement.ReadyCheck => 1
      case _ => throw new IllegalArgumentException(s"Unexpected query $query with ${s.getLog}")
    }

  def custom(getResult: TestState => Statement => Any): PureDAO = {
    def executeQuery(query: Statement): Pure[Any] =
      Pure((s: TestState) => (s.log(query), getResult(s)(query).asInstanceOf[Any]))

    def executeUpdate(sql: Statement): Pure[Int] =
      Pure((s: TestState) => (s.log(sql), 1))

    PureDAO(q => executeQuery(q), executeUpdate)
  }

  val init: PureDAO = custom(getResult)

  def interpreter(results: PureDAO): DAO[Pure] = new DAO[Pure] {
    def executeUpdate(sql: Statement, purpose: DAO.Purpose): Pure[Int] =
      results.executeUpdate(sql)

    def executeQuery[A](query: Statement)(implicit A: Read[A]): Pure[A] =
      results.executeQuery.asInstanceOf[Statement => Pure[A]](query)

    def executeQueryList[A](query: Statement)(implicit A: Read[A]): Pure[List[A]] =
      results.executeQuery.asInstanceOf[Statement => Pure[List[A]]](query)

    /*
    def executeQueryOption[A](query: Statement)(implicit A: Read[A]): Pure[Option[A]] =
      results.executeQuery.asInstanceOf[Statement => Pure[Option[A]]](query)
     */
  }

  val DummyTarget = new Target[Unit] {
    def toFragment(statement: Statement): Fragment =
      Fragment.const0(statement.toString)

    def getLoadStatements(
      discovery: DataDiscovery,
      eventTableColumns: EventTableColumns,
      i: Unit,
      disableRecovery: List[SchemaCriterion]
    ): LoadStatements =
      NonEmptyList(
        loadAuthMethod =>
          Statement.EventsCopy(
            discovery.base,
            Compression.Gzip,
            ColumnsToCopy(List.empty),
            ColumnsToSkip(List.empty),
            TypesInfo.Shredded(List.empty),
            loadAuthMethod,
            i
          ),
        discovery.shreddedTypes.map { shredded =>
          val discoveredShredModels = discovery.shredModels(shredded.info.getSchemaKey)
          val isRecovery = discoveredShredModels.shredModel match {
            case _: ShredModel.GoodModel => false
            case _: ShredModel.RecoveryModel => true
          }
          val isRecoveryDisabled = disableRecovery.contains(shredded.info.toCriterion)
          val tableName =
            if (isRecoveryDisabled) discoveredShredModels.mergeRedshiftSchemasResult.goodModel.tableName
            else discoveredShredModels.shredModel.tableName

          loadAuthMethod =>
            Statement.ShreddedCopy(shredded, Compression.Gzip, loadAuthMethod, discoveredShredModels.shredModel, tableName, isRecovery)
        }
      )

    def initQuery[F[_]: DAO: Monad]: F[Unit] = Monad[F].unit

    def getManifest: Statement =
      Statement.CreateTable(Fragment.const0("CREATE manifest"))

    override def getEventTable: Statement =
      Statement.CreateTable(Fragment.const0("CREATE events"))

    def updateTable(
      shredModel: ShredModel.GoodModel,
      currentSchemaKey: SchemaKey
    ): Migration.Block =
      throw new Throwable("Not implemented in test suite")

    def extendTable(info: ShreddedType.Info): List[Block] =
      throw new Throwable("Not implemented in test suite")

    def createTable(shredModel: ShredModel): Migration.Block = {
      val isRecovery = shredModel match {
        case ShredModel.GoodModel(_, _, _) => false
        case ShredModel.RecoveryModel(_, _, _) => true
      }
      val entity = Migration.Entity.Table("public", shredModel.schemaKey, shredModel.tableName)
      Block(Nil, List(Item.CreateTable(Fragment.const0(shredModel.toTableSql("public")), isRecovery)), entity)
    }

    def requiresEventsColumns: Boolean = false
  }
}
