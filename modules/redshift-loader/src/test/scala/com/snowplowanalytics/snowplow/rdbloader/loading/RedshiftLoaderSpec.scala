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
package com.snowplowanalytics.snowplow.rdbloader.loading

// This project
import com.snowplowanalytics.snowplow.loader.redshift.config.RedshiftTarget
import com.snowplowanalytics.snowplow.loader.redshift.db.{RsDao, Statement}
import com.snowplowanalytics.snowplow.loader.redshift.loading.{RedshiftLoader, RedshiftStatements}
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.SpecHelpers.AsSql
import com.snowplowanalytics.snowplow.rdbloader.config.components.PasswordConfig
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry
import com.snowplowanalytics.snowplow.rdbloader.test._
import org.specs2.mutable.Specification

class RedshiftLoaderSpec extends Specification {
  import RedshiftLoaderSpec._

  implicit val dao: RsDao[Pure] = PureDAO.interpreter(PureDAO.init)

  "loadFolder" should {
    "perform atomic and shredded insertions, ignore VACUUM and ANALYZE" >> {
      val builder: RedshiftLoader[Pure] = new RedshiftLoader[Pure](tgt, "eu-central-1")
      val input = RedshiftStatements(
        "atomic",
        Statement
          .EventsCopy("atomic", false, "s3://bucket/path/run=1/".dir, "eu-central-1", 1, "role", Compression.None),
        List(
          Statement.ShreddedCopy(
            "atomic",
            ShreddedType.Tabular(
              ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "context", 1, Semver(1, 0, 0))
            ),
            "eu-central-1",
            1,
            "role",
            Compression.None
          )
        )
      )

      val (state, result) = builder.loadFolder(input).run

      val expected = List(
        LogEntry.Message("setStage copying into events table"),
        LogEntry.Message("COPY atomic.events"),
        LogEntry.Message(
          Statement
            .EventsCopy("atomic", false, "s3://bucket/path/run=1/".dir, "eu-central-1", 1, "role", Compression.None)
            .toFragment
            .toString()
        ),
        LogEntry.Message(
          "COPY atomic.com_acme_context_1 FROM s3://bucket/path/run=1/output=good/vendor=com.acme/name=context/format=tsv/model=1"
        ),
        LogEntry.Message("setStage copying into com_acme_context_1 table"),
        LogEntry.Message(
          Statement
            .ShreddedCopy(
              "atomic",
              ShreddedType.Tabular(
                ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "context", 1, Semver(1, 0, 0))
              ),
              "eu-central-1",
              1,
              "role",
              Compression.None
            )
            .toFragment
            .toString()
        )
      )

      val transactionsExpectation = state.getLog must beEqualTo(expected)
      val resultExpectation       = result must beRight
      transactionsExpectation.and(resultExpectation)
    }

    "perform atomic transit load with shredded types, ignore VACUUM and ANALYZE" >> {
      val builder: RedshiftLoader[Pure] = new RedshiftLoader[Pure](tgt, "eu-central-1")

      val input = RedshiftStatements(
        "schema",
        Statement
          .EventsCopy("schema", true, "s3://bucket/path/run=1/".dir, "eu-central-1", 1, "role", Compression.None),
        List(
          Statement.ShreddedCopy(
            "schema",
            ShreddedType.Tabular(
              ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "context", 1, Semver(1, 0, 0))
            ),
            "eu-central-1",
            1,
            "role",
            Compression.None
          )
        )
      )

      val (state, result) = builder.loadFolder(input).run

      val expected = List(
        LogEntry.Message("setStage copying into events table"),
        LogEntry.Message("COPY schema.events (transit)"),
        LogEntry.Message(Statement.CreateTransient("schema").toFragment.toString()),
        LogEntry.Message(
          Statement
            .EventsCopy("schema", true, "s3://bucket/path/run=1/".dir, "eu-central-1", 1, "role", Compression.None)
            .toFragment
            .toString()
        ),
        LogEntry.Message(Statement.DropTransient("schema").toFragment.toString()),
        LogEntry.Message(
          "COPY schema.com_acme_context_1 FROM s3://bucket/path/run=1/output=good/vendor=com.acme/name=context/format=tsv/model=1"
        ),
        LogEntry.Message("setStage copying into com_acme_context_1 table"),
        LogEntry.Message(
          Statement
            .ShreddedCopy(
              "schema",
              ShreddedType.Tabular(
                ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "context", 1, Semver(1, 0, 0))
              ),
              "eu-central-1",
              1,
              "role",
              Compression.None
            )
            .toFragment
            .toString()
        )
      )

      state.getLog must beEqualTo(expected)
      result must beRight
    }
  }

  "run" should {
    "perform insertions, VACUUM and ANALYZE" >> {
      val builder: RedshiftLoader[Pure] = new RedshiftLoader[Pure](tgt, "us-east-1")

      val shreddedTypes = List(
        ShreddedType.Json(
          ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "event", 1, Semver(1, 5, 0)),
          "s3://assets/event_1.json".key
        ),
        ShreddedType.Json(
          ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "context", 2, Semver(1, 5, 0)),
          "s3://assets/context_2.json".key
        ),
        ShreddedType.Tabular(
          ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "context", 3, Semver(1, 5, 0))
        )
      )
      val discovery = DataDiscovery(S3.Folder.coerce("s3://bucket/path/run=1/"), shreddedTypes, Compression.Gzip)

      val (state, result) = builder.run(discovery).run

      val expected = List(
        LogEntry.Message("Loading s3://bucket/path/run=1/"),
        LogEntry.Message("setStage copying into events table"),
        LogEntry.Message("COPY atomic.events"),
        LogEntry.Message(
          Statement
            .EventsCopy(
              "atomic",
              false,
              "s3://bucket/path/run=1/".dir,
              "us-east-1",
              1,
              "arn:aws:iam::123456789876:role/RedshiftLoadRole",
              Compression.Gzip
            )
            .toFragment
            .toString()
        ),
        LogEntry.Message(
          "COPY atomic.com_acme_event_1 FROM s3://bucket/path/run=1/output=good/vendor=com.acme/name=event/format=json/model=1"
        ),
        LogEntry.Message("setStage copying into com_acme_event_1 table"),
        LogEntry.Message(
          Statement
            .ShreddedCopy(
              "atomic",
              ShreddedType.Json(
                ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "event", 1, Semver(1, 5, 0)),
                "s3://assets/event_1.json".key
              ),
              "us-east-1",
              1,
              "arn:aws:iam::123456789876:role/RedshiftLoadRole",
              Compression.Gzip
            )
            .toFragment
            .toString()
        ),
        LogEntry.Message(
          "COPY atomic.com_acme_context_2 FROM s3://bucket/path/run=1/output=good/vendor=com.acme/name=context/format=json/model=2"
        ),
        LogEntry.Message("setStage copying into com_acme_context_2 table"),
        LogEntry.Message(
          Statement
            .ShreddedCopy(
              "atomic",
              ShreddedType.Json(
                ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "context", 2, Semver(1, 5, 0)),
                "s3://assets/context_2.json".key
              ),
              "us-east-1",
              1,
              "arn:aws:iam::123456789876:role/RedshiftLoadRole",
              Compression.Gzip
            )
            .toFragment
            .toString()
        ),
        LogEntry.Message(
          "COPY atomic.com_acme_context_3 FROM s3://bucket/path/run=1/output=good/vendor=com.acme/name=context/format=tsv/model=3"
        ),
        LogEntry.Message("setStage copying into com_acme_context_3 table"),
        LogEntry.Message(
          Statement
            .ShreddedCopy(
              "atomic",
              ShreddedType.Tabular(
                ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "context", 3, Semver(1, 5, 0))
              ),
              "us-east-1",
              1,
              "arn:aws:iam::123456789876:role/RedshiftLoadRole",
              Compression.Gzip
            )
            .toFragment
            .toString()
        ),
        LogEntry.Message("Folder [s3://bucket/path/run=1/] has been loaded (not committed yet)")
      )

      val transactionsExpectation = state.getLog must beEqualTo(expected)
      val resultExpectation       = result must beRight
      transactionsExpectation.and(resultExpectation)
    }
  }
}

object RedshiftLoaderSpec {
  def setLoadingNoOp(table: String): Pure[Unit] = {
    val _ = table
    Pure.unit
  }

  val tgt = RedshiftTarget(
    "example.host",
    "ADD HERE",
    5439,
    RedshiftTarget.RedshiftJdbc.empty.copy(ssl = Some(true)),
    "arn:aws:iam::123456789876:role/RedshiftLoadRole",
    "atomic",
    "ADD HERE",
    PasswordConfig.PlainText("ADD HERE"),
    1,
    None
  )

}
