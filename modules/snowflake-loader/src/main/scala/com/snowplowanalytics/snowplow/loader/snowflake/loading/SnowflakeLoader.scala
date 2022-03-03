/*
 * Copyright (c) 2022-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.loader.snowflake.loading

import cats.MonadThrow
import cats.implicits._

import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent
import com.snowplowanalytics.snowplow.rdbloader.common.Common
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import com.snowplowanalytics.snowplow.rdbloader.state.Control
import com.snowplowanalytics.snowplow.rdbloader.algebras.db.TargetLoader
import com.snowplowanalytics.snowplow.rdbloader.loading.Stage
import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.loader.snowflake.db.DbUtils
import com.snowplowanalytics.snowplow.loader.snowflake.db.SfDao
import com.snowplowanalytics.snowplow.loader.snowflake.db.ast.AtomicDef
import com.snowplowanalytics.snowplow.loader.snowflake.db.Statement.CopyInto
import com.snowplowanalytics.snowplow.loader.snowflake.config.SnowflakeTarget

class SnowflakeLoader[C[_]: MonadThrow: Logging: SfDao: Control](target: SnowflakeTarget)
  extends TargetLoader[C] {

  import SnowflakeLoader._

  /**
    * Run loading actions for atomic and shredded data
    *
    * @param discovery batch discovered from message queue
    * @return block of statements to execute them out of a main transaction
    */
  def run(discovery: DataDiscovery): C[Unit] =
    shreddedTypeCheck(discovery.shreddedTypes) match {
      case Right(_) =>
        val copyStatement = getStatement(discovery, target)
        for {
          _ <- DbUtils.resumeWarehouse[C](target.warehouse)
          _ <- Logging[C].info(s"Loading ${discovery.base}")
          _ <- loadFolder(copyStatement)
          _ <- Logging[C].info(s"Folder [${discovery.base}] has been loaded (not committed yet)")
        } yield ()
      case Left(err) =>
        MonadThrow[C].raiseError(LoaderError.StorageTargetError(err))
    }

  def loadFolder(statement: CopyInto): C[Unit] =
    Control[C].setStage(Stage.Loading("events")) *>
      Logging[C].info(s"COPY events") *>
      SfDao[C].executeUpdate(statement).void
}

object SnowflakeLoader {
  val EventTable = "EVENTS"

  def getStatement(discovery: DataDiscovery, target: SnowflakeTarget): CopyInto = {
    val columns = getColumns(discovery)
    val loadPath = s"${discovery.runId}/${Common.GoodPrefix}"
    CopyInto(
      target.schema,
      EventTable,
      target.transformedStage,
      columns,
      loadPath,
      target.maxError
    )
  }

  def getColumns(discovery: DataDiscovery): List[String] = {
    val atomicColumns = AtomicDef.columns.map(_.name)
    val shredTypeColumns = discovery.shreddedTypes
      .filterNot(_.isAtomic)
      .map(getShredTypeColumn)
    atomicColumns ::: shredTypeColumns
  }

  def getShredTypeColumn(shreddedType: ShreddedType): String = {
    val shredProperty = shreddedType.getSnowplowEntity.toSdkProperty
    val info = shreddedType.info
    SnowplowEvent.transformSchema(shredProperty, info.vendor, info.name, info.model)
  }

  def shreddedTypeCheck(shreddedTypes: List[ShreddedType]): Either[String, Unit] = {
    val unsupportedType = shreddedTypes.exists {
      case _: ShreddedType.Tabular | _: ShreddedType.Json => true
      case _: ShreddedType.Widerow => false
    }
    if (unsupportedType) "Snowflake Loader supports types with widerow format only. Received discovery contains types with unsupported format.".asLeft
    else ().asRight
  }
}
