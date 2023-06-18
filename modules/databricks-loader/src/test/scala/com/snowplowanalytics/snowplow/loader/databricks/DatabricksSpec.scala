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
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow.WideRowFormat.PARQUET
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.Region
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{SnowplowEntity, TypesInfo}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Columns.{ColumnName, ColumnsToCopy, ColumnsToSkip}
import com.snowplowanalytics.snowplow.rdbloader.db.{Statement, Target}
import com.snowplowanalytics.snowplow.rdbloader.cloud.authservice.LoadAuthService.LoadAuthMethod
import com.snowplowanalytics.snowplow.rdbloader.ConfigSpec._

import java.time.Instant
import scala.concurrent.duration.DurationInt
import org.specs2.mutable.Specification

class DatabricksSpec extends Specification {
  import DatabricksSpec._

  "getLoadStatements" should {

    // The essence of this test (column name generation) is now handled by the NonAtomicFieldProvider, which has
    // its own tests. Refer to `NonAtomicFieldProviderSpec.scala` in `common`
    "create LoadStatements with columns to copy and columns to skip" in {

      val eventsColumns = List(
        "unstruct_event_com_acme_aaa_1",
        "unstruct_event_com_acme_bbb_1",
        "contexts_com_acme_xxx_1",
        "contexts_com_acme_yyy_1",
        "not_a_snowplow_column"
      ).map(ColumnName)

      val shreddedTypes = List(
        ShreddedType.Widerow(ShreddedType.Info(baseFolder, "com_acme", "aaa", 1, SnowplowEntity.SelfDescribingEvent)),
        ShreddedType.Widerow(ShreddedType.Info(baseFolder, "com_acme", "ccc", 1, SnowplowEntity.SelfDescribingEvent)),
        ShreddedType.Widerow(ShreddedType.Info(baseFolder, "com_acme", "yyy", 1, SnowplowEntity.Context)),
        ShreddedType.Widerow(ShreddedType.Info(baseFolder, "com_acme", "zzz", 1, SnowplowEntity.Context))
      )

      val discovery = DataDiscovery(
        baseFolder,
        shreddedTypes,
        Compression.Gzip,
        TypesInfo.WideRow(PARQUET, List.empty),
        List(
          "unstruct_event_com_acme_aaa_1",
          "unstruct_event_com_acme_ccc_1",
          "contexts_com_acme_yyy_1",
          "contexts_com_acme_zzz_1"
        )
      )

      val results = target
        .getLoadStatements(discovery, eventsColumns, ())
        .map(f => f(LoadAuthMethod.NoCreds))

      results should be like { case NonEmptyList(Statement.EventsCopy(path, compression, columnsToCopy, columnsToSkip, _, _, _), Nil) =>
        path must beEqualTo(baseFolder)
        compression must beEqualTo(Compression.Gzip)

        columnsToCopy.names must contain(
          allOf(
            ColumnName("unstruct_event_com_acme_aaa_1"),
            ColumnName("unstruct_event_com_acme_ccc_1"),
            ColumnName("contexts_com_acme_yyy_1"),
            ColumnName("contexts_com_acme_zzz_1")
          )
        )

        columnsToCopy.names must not contain (ColumnName("unstruct_event_com_acme_bbb_1"))
        columnsToCopy.names must not contain (ColumnName("contexts_com_acme_xxx_1"))
        columnsToCopy.names must not contain (ColumnName("not_a_snowplow_column"))

        columnsToSkip.names must beEqualTo(
          List(
            ColumnName("unstruct_event_com_acme_bbb_1"),
            ColumnName("contexts_com_acme_xxx_1")
          )
        )
      }
    }
  }

  "toFragment" should {
    "create sql for loading" in {
      val toCopy = ColumnsToCopy(
        List(
          ColumnName("app_id"),
          ColumnName("unstruct_event_com_acme_aaa_1"),
          ColumnName("contexts_com_acme_xxx_1")
        )
      )
      val toSkip = ColumnsToSkip(
        List(
          ColumnName("unstruct_event_com_acme_bbb_1"),
          ColumnName("contexts_com_acme_yyy_1")
        )
      )
      val statement =
        Statement.EventsCopy(
          baseFolder,
          Compression.Gzip,
          toCopy,
          toSkip,
          TypesInfo.WideRow(PARQUET, List.empty),
          LoadAuthMethod.NoCreds,
          ()
        )

      target.toFragment(statement).toString must beLike { case sql =>
        sql must contain(
          "SELECT app_id,unstruct_event_com_acme_aaa_1,contexts_com_acme_xxx_1,NULL AS unstruct_event_com_acme_bbb_1,NULL AS contexts_com_acme_yyy_1,current_timestamp() AS load_tstamp from 's3://somewhere/path/output=good/'"
        )
      }
    }

    "create sql with credentials for loading from S3" in {
      val toCopy = ColumnsToCopy(
        List(
          ColumnName("app_id"),
          ColumnName("unstruct_event_com_acme_aaa_1"),
          ColumnName("contexts_com_acme_xxx_1")
        )
      )
      val toSkip = ColumnsToSkip(
        List(
          ColumnName("unstruct_event_com_acme_bbb_1"),
          ColumnName("contexts_com_acme_yyy_1")
        )
      )
      val loadAuthMethod = LoadAuthMethod.TempCreds.AWS("testAccessKey", "testSecretKey", "testSessionToken", Instant.now.plusSeconds(3600))
      val statement =
        Statement.EventsCopy(baseFolder, Compression.Gzip, toCopy, toSkip, TypesInfo.WideRow(PARQUET, List.empty), loadAuthMethod, ())

      target.toFragment(statement).toString must beLike { case sql =>
        sql must contain(
          s"SELECT app_id,unstruct_event_com_acme_aaa_1,contexts_com_acme_xxx_1,NULL AS unstruct_event_com_acme_bbb_1,NULL AS contexts_com_acme_yyy_1,current_timestamp() AS load_tstamp from 's3://somewhere/path/output=good/' WITH ( CREDENTIAL (AWS_ACCESS_KEY = '${loadAuthMethod.awsAccessKey}', AWS_SECRET_KEY = '${loadAuthMethod.awsSecretKey}', AWS_SESSION_TOKEN = '${loadAuthMethod.awsSessionToken}') )"
        )
      }
    }

    "create sql with credentials for loading from Azure Blob Storage" in {
      val toCopy = ColumnsToCopy(
        List(
          ColumnName("app_id"),
          ColumnName("unstruct_event_com_acme_aaa_1"),
          ColumnName("contexts_com_acme_xxx_1")
        )
      )
      val toSkip = ColumnsToSkip(
        List(
          ColumnName("unstruct_event_com_acme_bbb_1"),
          ColumnName("contexts_com_acme_yyy_1")
        )
      )
      val baseFolder: BlobStorage.Folder =
        BlobStorage.Folder.coerce("https://test.blob.core.windows.net/test-container/path1/path2")
      val loadAuthMethod = LoadAuthMethod.TempCreds.Azure("testToken", Instant.now.plusSeconds(3600))
      val statement =
        Statement.EventsCopy(baseFolder, Compression.Gzip, toCopy, toSkip, TypesInfo.WideRow(PARQUET, List.empty), loadAuthMethod, ())

      target.toFragment(statement).toString must beLike { case sql =>
        sql must contain(
          s"SELECT app_id,unstruct_event_com_acme_aaa_1,contexts_com_acme_xxx_1,NULL AS unstruct_event_com_acme_bbb_1,NULL AS contexts_com_acme_yyy_1,current_timestamp() AS load_tstamp from 'abfss://test-container@test.dfs.core.windows.net/path1/path2/output=good/' WITH ( CREDENTIAL (AZURE_SAS_TOKEN = '${loadAuthMethod.sasToken}') )"
        )
      }
    }
  }
}

object DatabricksSpec {

  val baseFolder: BlobStorage.Folder =
    BlobStorage.Folder.coerce("s3://somewhere/path")

  val target: Target[Unit] = Databricks
    .build(
      Config(
        StorageTarget.Databricks(
          "host",
          None,
          "snowplow",
          443,
          "some/path",
          StorageTarget.PasswordConfig.PlainText("xxx"),
          None,
          "useragent",
          StorageTarget.LoadAuthMethod.NoCreds,
          2.days,
          logLevel = 3
        ),
        Config.Cloud.AWS(
          Region("eu-central-1"),
          Config.Cloud.AWS.SQS("my-queue.fifo", Some(Region("eu-central-1")))
        ),
        None,
        Config.Monitoring(None, None, Config.Metrics(None, None, 1.minute), None, None, None),
        None,
        Config.Schedules(Nil),
        Config.Timeouts(1.minute, 1.minute, 1.minute, 1.minute, 30.seconds),
        Config.Retries(Config.Strategy.Constant, None, 1.minute, None),
        Config.Retries(Config.Strategy.Constant, None, 1.minute, None),
        Config.Retries(Config.Strategy.Constant, None, 1.minute, None),
        Config.FeatureFlags(addLoadTstampColumn = true),
        exampleTelemetry
      )
    )
    .right
    .get

}
