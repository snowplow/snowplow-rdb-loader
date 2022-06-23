/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.loader.databricks

import cats.data.NonEmptyList

import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.Region
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.SnowplowEntity
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.{Statement, Target}

import scala.concurrent.duration.DurationInt

import org.specs2.mutable.Specification


class DatabricksSpec extends Specification {
  import DatabricksSpec._

  "getLoadStatements" should {

    "create LoadStatements with columns to copy and columns to skip" in {

      val eventsColumns = List(
        "unstruct_event_com_acme_aaa_1",
        "unstruct_event_com_acme_bbb_1",
        "contexts_com_acme_xxx_1",
        "contexts_com_acme_yyy_1",
        "not_a_snowplow_column"
      )

      val shreddedTypes = List(
        ShreddedType.Widerow(ShreddedType.Info(baseFolder, "com_acme", "aaa", 1, SnowplowEntity.SelfDescribingEvent)),
        ShreddedType.Widerow(ShreddedType.Info(baseFolder, "com_acme", "ccc", 1, SnowplowEntity.SelfDescribingEvent)),
        ShreddedType.Widerow(ShreddedType.Info(baseFolder, "com_acme", "yyy", 1, SnowplowEntity.Context)),
        ShreddedType.Widerow(ShreddedType.Info(baseFolder, "com_acme", "zzz", 1, SnowplowEntity.Context))
      )

      val discovery = DataDiscovery(baseFolder, shreddedTypes, Compression.Gzip)

      target.getLoadStatements(discovery, eventsColumns) should be like {
        case NonEmptyList(Statement.EventsCopy(path, compression, columnsToCopy, columnsToSkip), Nil) =>
          path must beEqualTo(baseFolder)
          compression must beEqualTo(Compression.Gzip)

          columnsToCopy must contain(allOf(
            "unstruct_event_com_acme_aaa_1",
            "unstruct_event_com_acme_ccc_1",
            "contexts_com_acme_yyy_1",
            "contexts_com_acme_zzz_1",
          ))

          columnsToCopy must not contain("unstruct_event_com_acme_bbb_1")
          columnsToCopy must not contain("contexts_com_acme_xxx_1")
          columnsToCopy must not contain("not_a_snowplow_column")

          columnsToSkip must beEqualTo(List(
            "unstruct_event_com_acme_bbb_1",
            "contexts_com_acme_xxx_1",
          ))
      }
    }
  }

  "toFragment" should {
    "create sql for loading" in {
      val toCopy = List(
        "app_id",
        "unstruct_event_com_acme_aaa_1",
        "contexts_com_acme_xxx_1"
      )
      val toSkip = List(
        "unstruct_event_com_acme_bbb_1",
        "contexts_com_acme_yyy_1"
      )
      val statement = Statement.EventsCopy(baseFolder, Compression.Gzip, toCopy, toSkip)

      target.toFragment(statement).toString must beLike { case sql =>
        sql must contain("SELECT app_id,unstruct_event_com_acme_aaa_1,contexts_com_acme_xxx_1,NULL AS unstruct_event_com_acme_bbb_1,NULL AS contexts_com_acme_yyy_1,current_timestamp() AS load_tstamp from 's3://somewhere/path/output=good/'")
      }
    }
  }
}

object DatabricksSpec {

  val baseFolder: S3.Folder =
    S3.Folder.coerce("s3://somewhere/path")

  val target: Target = Databricks.build(Config(
    Region("eu-central-1"),
    None,
    Config.Monitoring(None, None, Config.Metrics(None, None, 1.minute), None, None, None),
    "my-queue.fifo",
    None,
    StorageTarget.Databricks(
      "host",
      "hive_metastore",
      "snowplow",
      443,
      "some/path",
      StorageTarget.PasswordConfig.PlainText("xxx"),
      None,
      "useragent"
    ),
    Config.Schedules(Nil),
    Config.Timeouts(1.minute, 1.minute, 1.minute),
    Config.Retries(Config.Strategy.Constant, None, 1.minute, None),
    Config.Retries(Config.Strategy.Constant, None, 1.minute, None)
  )).right.get

}
