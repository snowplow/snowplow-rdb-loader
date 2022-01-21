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
package com.snowplowanalytics.snowplow.loader.redshift.config

import java.util.Properties
import io.circe._
import io.circe.Decoder._
import io.circe.generic.semiauto._
import com.snowplowanalytics.snowplow.rdbloader.common.config.StringEnum
import com.snowplowanalytics.snowplow.rdbloader.config.{SecretExtractor, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.config.components.{PasswordConfig, TunnelConfig}

/**
  * Common configuration for JDBC target, such as Redshift
  * Any of those can be safely coerced
  */
final case class RedshiftTarget(
  host: String,
  database: String,
  port: Int,
  jdbc: RedshiftTarget.RedshiftJdbc,
  roleArn: String,
  schema: String,
  username: String,
  password: PasswordConfig,
  maxError: Int,
  sshTunnel: Option[TunnelConfig]
) extends StorageTarget {
  def shreddedTable(tableName: String): String = s"$schema.$tableName"
}

object RedshiftTarget {

  implicit val secretExtractor: SecretExtractor[RedshiftTarget] = new SecretExtractor[RedshiftTarget] {
    override def extract(c: RedshiftTarget): List[String] = List(c.username, c.password.getUnencrypted)
  }

  final case class ParseError(message: String) extends AnyVal

  sealed trait SslMode extends StringEnum {
    def asProperty: String = asString.toLowerCase.replace('_', '-')
  }

  implicit val sslModeDecoder: Decoder[SslMode] =
    StringEnum.decodeStringEnum[SslMode]

  object SslMode {
    final case object Disable extends SslMode { def asString    = "DISABLE" }
    final case object Require extends SslMode { def asString    = "REQUIRE" }
    final case object VerifyCa extends SslMode { def asString   = "VERIFY_CA" }
    final case object VerifyFull extends SslMode { def asString = "VERIFY_FULL" }
  }

  /**
    * All possible JDBC according to Redshift documentation, except deprecated
    * and authentication-related
    */
  final case class RedshiftJdbc(
    blockingRows: Option[Int],
    disableIsValidQuery: Option[Boolean],
    dsiLogLevel: Option[Int],
    filterLevel: Option[String],
    loginTimeout: Option[Int],
    loglevel: Option[Int],
    socketTimeout: Option[Int],
    ssl: Option[Boolean],
    sslMode: Option[String],
    sslRootCert: Option[String],
    tcpKeepAlive: Option[Boolean],
    tcpKeepAliveMinutes: Option[Int]
  ) {
    val properties: Properties = {
      val props: Properties = new Properties()
      props.add("BlockingRowsMode", blockingRows)
      props.add("DisableIsValidQuery", disableIsValidQuery)
      props.add("DSILogLevel", dsiLogLevel)
      props.add("FilterLevel", filterLevel)
      props.add("loginTimeout", loginTimeout)
      props.add("loglevel", loglevel)
      props.add("socketTimeout", socketTimeout)
      props.add("ssl", ssl)
      props.add("sslMode", sslMode)
      props.add("sslRootCert", sslRootCert)
      props.add("tcpKeepAlive", tcpKeepAlive)
      props.add("TCPKeepAliveMinutes", tcpKeepAliveMinutes)
      props
    }
  }

  object RedshiftJdbc {
    val empty = RedshiftJdbc(None, None, None, None, None, None, None, None, None, None, None, None)

    implicit def jdbcDecoder: Decoder[RedshiftJdbc] =
      Decoder.forProduct12(
        "BlockingRowsMode",
        "DisableIsValidQuery",
        "DSILogLevel",
        "FilterLevel",
        "loginTimeout",
        "loglevel",
        "socketTimeout",
        "ssl",
        "sslMode",
        "sslRootCert",
        "tcpKeepAlive",
        "TCPKeepAliveMinutes"
      )(RedshiftJdbc.apply)

    implicit def jdbcEncoder: Encoder.AsObject[RedshiftJdbc] =
      Encoder.forProduct12(
        "BlockingRowsMode",
        "DisableIsValidQuery",
        "DSILogLevel",
        "FilterLevel",
        "loginTimeout",
        "loglevel",
        "socketTimeout",
        "ssl",
        "sslMode",
        "sslRootCert",
        "tcpKeepAlive",
        "TCPKeepAliveMinutes"
      )((j: RedshiftJdbc) =>
        (
          j.blockingRows,
          j.disableIsValidQuery,
          j.dsiLogLevel,
          j.filterLevel,
          j.loginTimeout,
          j.loglevel,
          j.socketTimeout,
          j.ssl,
          j.sslMode,
          j.sslRootCert,
          j.tcpKeepAlive,
          j.tcpKeepAliveMinutes
        )
      )
  }
  implicit class jdbcPropertySettings(properties: Properties) {
    def add[T](fieldKey: String, fieldValue: Option[T]): Unit =
      fieldValue.foreach(t => properties.setProperty(fieldKey, t.toString))
  }

  implicit lazy val redshiftConfigDecoder: Decoder[RedshiftTarget] = deriveDecoder[RedshiftTarget]
}
