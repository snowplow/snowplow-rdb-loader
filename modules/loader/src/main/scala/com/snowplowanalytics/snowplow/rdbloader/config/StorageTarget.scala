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
package com.snowplowanalytics.snowplow.rdbloader.config

import java.util.Properties

import cats.data._
import cats.implicits._

import io.circe.{CursorOp, _}
import io.circe.Decoder._
import io.circe.generic.semiauto._

import com.snowplowanalytics.snowplow.rdbloader.common.config.StringEnum

/**
 * Common configuration for JDBC target, such as Redshift
 * Any of those can be safely coerced
 */
sealed trait StorageTarget extends Product with Serializable {
  def host: String
  def database: String
  def schema: String
  def port: Int
  def username: String
  def password: PasswordConfig
  def sshTunnel: Option[TunnelConfig]

  def shreddedTable(tableName: String): String =
    s"$schema.$tableName"
}

object StorageTarget {

  final case class ParseError(message: String) extends AnyVal

  sealed trait SslMode extends StringEnum {
    def asProperty = asString.toLowerCase.replace('_', '-')
  }

  implicit val sslModeDecoder: Decoder[SslMode] =
    StringEnum.decodeStringEnum[SslMode]

  object SslMode {
    final case object Disable extends SslMode { def asString = "DISABLE" }
    final case object Require extends SslMode { def asString = "REQUIRE" }
    final case object VerifyCa extends SslMode { def asString = "VERIFY_CA" }
    final case object VerifyFull extends SslMode { def asString = "VERIFY_FULL" }
  }

  /** Amazon Redshift connection settings */
  final case class Redshift(host: String,
                            database: String,
                            port: Int,
                            jdbc: RedshiftJdbc,
                            roleArn: String,
                            schema: String,
                            username: String,
                            password: PasswordConfig,
                            maxError: Int,
                            sshTunnel: Option[TunnelConfig])
    extends StorageTarget

  /**
    * All possible JDBC according to Redshift documentation, except deprecated
    * and authentication-related
    */
  final case class RedshiftJdbc(blockingRows: Option[Int],
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
                          tcpKeepAliveMinutes: Option[Int]) {
    /** Either errors or list of mutators to update the `Properties` object */
    val validation: Either[ParseError, List[Properties => Unit]] = RedshiftJdbc.jdbcEncoder.encodeObject(this).toList.map {
      case (property, value) => value.fold(
        ((_: Properties) => ()).asRight,
        b => ((props: Properties) => { props.setProperty(property, b.toString); () }).asRight,
        n => n.toInt match {
          case Some(num) =>
            ((props: Properties) => {
              props.setProperty(property, num.toString)
              ()
            }).asRight
          case None => s"Impossible to apply JDBC property [$property] with value [${value.noSpaces}]".asLeft
        },
        s => ((props: Properties) => { props.setProperty(property, s); ()}).asRight,
        _ => s"Impossible to apply JDBC property [$property] with JSON array".asLeft,
        _ => s"Impossible to apply JDBC property [$property] with JSON object".asLeft
      )
    } traverse(_.toValidatedNel) match {
      case Validated.Valid(updaters) => updaters.asRight[ParseError]
      case Validated.Invalid(errors) =>
        val messages = "Invalid JDBC options: " ++ errors.toList.mkString(", ")
        val error: ParseError = ParseError(messages)
        error.asLeft[List[Properties => Unit]]
    }
  }

  object RedshiftJdbc {
    val empty = RedshiftJdbc(None, None, None, None, None, None, None, None, None, None, None, None)

    implicit def jdbcDecoder: Decoder[RedshiftJdbc] =
      Decoder.forProduct12("BlockingRowsMode", "DisableIsValidQuery", "DSILogLevel",
        "FilterLevel", "loginTimeout", "loglevel", "socketTimeout", "ssl", "sslMode",
        "sslRootCert", "tcpKeepAlive", "TCPKeepAliveMinutes")(RedshiftJdbc.apply)

    implicit def jdbcEncoder: Encoder.AsObject[RedshiftJdbc] =
      Encoder.forProduct12("BlockingRowsMode", "DisableIsValidQuery", "DSILogLevel",
        "FilterLevel", "loginTimeout", "loglevel", "socketTimeout", "ssl", "sslMode",
        "sslRootCert", "tcpKeepAlive", "TCPKeepAliveMinutes")((j: RedshiftJdbc) =>
        (j.blockingRows, j.disableIsValidQuery, j.dsiLogLevel,
          j.filterLevel, j.loginTimeout, j.loglevel, j.socketTimeout, j.ssl, j.sslMode,
          j.sslRootCert, j.tcpKeepAlive, j.tcpKeepAliveMinutes))
  }

  implicit def redsfhitConfigDecoder: Decoder[Redshift] =
    deriveDecoder[Redshift]

  implicit def storageTargetDecoder: Decoder[StorageTarget] =
    Decoder.instance { cur =>
      val typeCur = cur.downField("type")
      typeCur.as[String] match {
        case Right("redshift") =>
          cur.as[Redshift]
        case Right(other) =>
          Left(DecodingFailure(s"Storage target of type $other is not supported yet", typeCur.history))
        case Left(DecodingFailure(_, List(CursorOp.DownField("type")))) =>
          Left(DecodingFailure("Cannot find 'type' string in storage configuration", typeCur.history))
        case Left(other) =>
          Left(other)
      }
    }
}
