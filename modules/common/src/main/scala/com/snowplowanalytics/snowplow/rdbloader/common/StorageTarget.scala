/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.common

import java.util.Properties

import cats.data._
import cats.implicits._

import io.circe._
import io.circe.CursorOp
import io.circe.Decoder._
import io.circe.generic.semiauto._

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
  def password: StorageTarget.PasswordConfig
  def sshTunnel: Option[StorageTarget.TunnelConfig]

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
                            compRows: Long,
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

  /** Reference to encrypted entity inside EC2 Parameter Store */
  final case class ParameterStoreConfig(parameterName: String)

  /** Reference to encrypted key (EC2 Parameter Store only so far) */
  final case class EncryptedConfig(ec2ParameterStore: ParameterStoreConfig)

  /** Bastion host access configuration for SSH tunnel */
  final case class BastionConfig(host: String, port: Int, user: String, passphrase: Option[String], key: Option[EncryptedConfig])

  /** Destination socket for SSH tunnel - usually DB socket inside private network */
  final case class DestinationConfig(host: String, port: Int)

  /** ADT representing fact that password can be either plain-text or encrypted in EC2 Parameter Store */
  sealed trait PasswordConfig extends Product with Serializable {
    def getUnencrypted: String = this match {
      case PasswordConfig.PlainText(plain) => plain
      case PasswordConfig.EncryptedKey(EncryptedConfig(key)) => key.parameterName
    }
  }
  object PasswordConfig {
    final case class PlainText(value: String) extends PasswordConfig
    final case class EncryptedKey(value: EncryptedConfig) extends PasswordConfig

    implicit object PasswordDecoder extends Decoder[PasswordConfig] {
      def apply(hCursor: HCursor): Decoder.Result[PasswordConfig] = {
        hCursor.value.asString match {
          case Some(s) => Right(PasswordConfig.PlainText(s))
          case None => hCursor.value.asObject match {
            case Some(_) => hCursor.value.as[EncryptedConfig].map(PasswordConfig.EncryptedKey)
            case None => Left(DecodingFailure("password should be either plain text or reference to encrypted key", hCursor.history))
          }
        }
      }
    }
  }

  /**
    * SSH configuration, enabling target to be loaded though tunnel
    *
    * @param bastion bastion host SSH configuration
    * @param localPort local port to which RDB Loader should connect,
    *                  same port as in `StorageTarget`, can be arbitrary
    * @param destination end-socket of SSH tunnel (host/port pair to access DB)
    */
  final case class TunnelConfig(bastion: BastionConfig, localPort: Int, destination: DestinationConfig)

  implicit def redsfhitConfigDecoder: Decoder[Redshift] =
    deriveDecoder[Redshift]

  implicit def encryptedConfigDecoder: Decoder[EncryptedConfig] =
    deriveDecoder[EncryptedConfig]

  implicit def tunnerConfigDecoder: Decoder[TunnelConfig] =
    deriveDecoder[TunnelConfig]

  implicit def bastionConfigDecoder: Decoder[BastionConfig] =
    deriveDecoder[BastionConfig]

  implicit def destinationConfigDecoder: Decoder[DestinationConfig] =
    deriveDecoder[DestinationConfig]

  implicit def parameterStoreConfigDecoder: Decoder[ParameterStoreConfig] =
    deriveDecoder[ParameterStoreConfig]

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
