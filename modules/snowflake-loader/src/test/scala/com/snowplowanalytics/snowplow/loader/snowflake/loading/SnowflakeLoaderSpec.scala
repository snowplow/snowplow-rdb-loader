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

import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, Common}
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.config.components.PasswordConfig
import com.snowplowanalytics.snowplow.rdbloader.discovery.{ShreddedType, DataDiscovery}
import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.SpecHelpers.AsSql
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry
import com.snowplowanalytics.snowplow.rdbloader.test._

import com.snowplowanalytics.snowplow.loader.snowflake.config.SnowflakeTarget
import com.snowplowanalytics.snowplow.loader.snowflake.db.{SfDao, Statement}
import com.snowplowanalytics.snowplow.loader.snowflake.ast.AtomicDef
import com.snowplowanalytics.snowplow.loader.snowflake.test._

import org.specs2.mutable.Specification

class SnowflakeLoaderSpec extends Specification {
  import SnowflakeLoaderSpec._

  "run" should {
    "perform COPY" >> {
      implicit val dao: SfDao[Pure] = PureDAO.interpreter(PureDAO.init)
      lazy val loader = new SnowflakeLoader[Pure](target)

      val shreddedTypes = List(
        ShreddedType.Widerow(
          ShreddedType.Info(base.dir, "com.acme", "event", 1, shredJobVersion, LoaderMessage.SnowplowEntity.SelfDescribingEvent)
        ),
        ShreddedType.Widerow(
          ShreddedType.Info(base.dir, "com.acme", "context", 1, shredJobVersion, LoaderMessage.SnowplowEntity.SelfDescribingEvent)
        )
      )
      val discovery = DataDiscovery(base.dir, shreddedTypes, Compression.Gzip)

      val (state, result) = loader.run(discovery).run

      val expected = List(
        LogEntry.Message(Statement.WarehouseResume(target.warehouse).toTestString),
        LogEntry.Message(s"Loading $base"),
        LogEntry.Message("setStage copying into events table"),
        LogEntry.Message("COPY events"),
        LogEntry.Message(
          Statement.CopyInto(
            target.schema,
            SnowflakeLoader.EventTable,
            target.transformedStage,
            SnowflakeLoader.getColumns(discovery),
            loadPath,
            target.maxError
          ).toTestString
        ),
        LogEntry.Message(s"Folder [$base] has been loaded (not committed yet)")
      )

      state.getLog must beEqualTo(expected)
      result must beRight
    }

    "throw error when discovery contains shredded types with unsupported formats" >> {
      implicit val dao: SfDao[Pure] = PureDAO.interpreter(PureDAO.init)
      lazy val loader = new SnowflakeLoader[Pure](target)

      val shreddedTypes = List(
        ShreddedType.Widerow(
          ShreddedType.Info(base.dir, "com.acme", "event", 1, shredJobVersion, LoaderMessage.SnowplowEntity.SelfDescribingEvent)
        ),
        ShreddedType.Tabular(
          ShreddedType.Info(base.dir, "com.acme", "context", 1, shredJobVersion, LoaderMessage.SnowplowEntity.SelfDescribingEvent)
        )
      )
      val discovery = DataDiscovery(base.dir, shreddedTypes, Compression.Gzip)

      val (state, result) = loader.run(discovery).run

      state.getLog must beEmpty
      result must beLeft.like {
        case LoaderError.StorageTargetError(_) => ok
      }
    }
  }

  "getColumn" should {
    "return list which contains atomic columns and columns for shredded types in discovery" >> {
      val shreddedTypes = List(
        ShreddedType.Widerow(
          ShreddedType.Info(base.dir, "com.acme", "event", 1, shredJobVersion, LoaderMessage.SnowplowEntity.SelfDescribingEvent)
        ),
        ShreddedType.Widerow(
          ShreddedType.Info(base.dir, "com.acme", "context", 1, shredJobVersion, LoaderMessage.SnowplowEntity.Context)
        )
      )
      val discovery = DataDiscovery(base.dir, shreddedTypes, Compression.Gzip)

      val columns = SnowflakeLoader.getColumns(discovery)

      val expected = AtomicDef.columns.map(_.name) :::
        List(
          "unstruct_event_com_acme_event_1",
          "contexts_com_acme_context_1"
        )

      columns must beEqualTo(expected)
    }
  }

  "Helper methods of CopyInto" should {
    val columns = List("col1", "col2", "col_3")
    "return in expected format" >> {
      val copyInto = Statement.CopyInto(
        target.schema,
        SnowflakeLoader.EventTable,
        target.transformedStage,
        columns,
        loadPath,
        target.maxError
      )

      val expectedColumnsForCopy = "col1,col2,col_3"
      val expectedColumnsForSelect = "$1:col1,$1:col2,$1:col_3"

      copyInto.columnsForCopy must beEqualTo(expectedColumnsForCopy)
      copyInto.columnsForSelect must beEqualTo(expectedColumnsForSelect)
    }
  }
}

object SnowflakeLoaderSpec {
  val target = SnowflakeTarget(
    "us-west-2",
    "sf-user",
    Some("sf-role"),
    PasswordConfig.PlainText("sf-pass"),
    "sf-account",
    "sf-warehouse",
    "sf-database",
    "sf-schema",
    "sf-transformed-stage",
    "sf-loader",
    Some("sf-monitoring-stage"),
    Some(10),
    None
  )
  val runId = "run=1"
  val loadPath = s"$runId/${Common.GoodPrefix}"
  val base = s"s3://bucket/path/$runId/"
  val shredProperty = LoaderMessage.SnowplowEntity.SelfDescribingEvent
  val shredJobVersion = Semver(1, 5, 0)
}
