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
import com.snowplowanalytics.snowplow.loader.snowflake.db.ast.AtomicDef
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
          ShreddedType.Info(base.dir, "com.acme", "event", 1, shredJobVersion, LoaderMessage.ShreddedType.SelfDescribingEvent)
        ),
        ShreddedType.Widerow(
          ShreddedType.Info(base.dir, "com.acme", "context", 1, shredJobVersion, LoaderMessage.ShreddedType.SelfDescribingEvent)
        )
      )
      val discovery = DataDiscovery(base.dir, shreddedTypes, Compression.Gzip)

      val (state, result) = loader.run(discovery).run

      val expected = List(
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
          ShreddedType.Info(base.dir, "com.acme", "event", 1, shredJobVersion, LoaderMessage.ShreddedType.SelfDescribingEvent)
        ),
        ShreddedType.Tabular(
          ShreddedType.Info(base.dir, "com.acme", "context", 1, shredJobVersion, LoaderMessage.ShreddedType.SelfDescribingEvent)
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
          ShreddedType.Info(base.dir, "com.acme", "event", 1, shredJobVersion, LoaderMessage.ShreddedType.SelfDescribingEvent)
        ),
        ShreddedType.Widerow(
          ShreddedType.Info(base.dir, "com.acme", "context", 1, shredJobVersion, LoaderMessage.ShreddedType.Contexts)
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
    SnowflakeTarget.AuthMethod.StageAuth,
    "us-west-2",
    "sf-user",
    PasswordConfig.PlainText("sf-pass"),
    "sf-account",
    "sf-warehouse",
    "sf-database",
    "sf-schema",
    "sf-transformed-stage",
    "sf-monitoring-stage",
    Some(10),
    None
  )
  val runId = "run=1"
  val loadPath = s"$runId/${Common.GoodPrefix}"
  val base = s"s3://bucket/path/$runId/"
  val shredProperty = LoaderMessage.ShreddedType.SelfDescribingEvent
  val shredJobVersion = Semver(1, 5, 0)
}
