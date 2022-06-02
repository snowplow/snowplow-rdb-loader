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

import doobie.free.connection.setAutoCommit
import doobie.util.transactor.Strategy
import doobie.util.fragment.Fragment
import doobie.implicits._

import com.snowplowanalytics.snowplow.rdbloader.common.config.StringEnum


/**
  * Common configuration for JDBC target, such as Redshift
  * Any of those can be safely coerced
  */
sealed trait StorageTarget extends Product with Serializable {
  def schema: String
  def username: String
  def password: StorageTarget.PasswordConfig
  def sshTunnel: Option[StorageTarget.TunnelConfig]

  def shreddedTable(tableName: String): String =
    s"$schema.$tableName"

  def doobieCommitStrategy: Strategy = Strategy.default

  /**
    * Surprisingly, for statements disallowed in transaction block we need to set autocommit
    * @see https://awsbytes.com/alter-table-alter-column-cannot-run-inside-a-transaction-block/
    */
  def doobieNoCommitStrategy: Strategy = Strategy.void.copy(before = setAutoCommit(true), always = setAutoCommit(false))
  def driver: String
  def withAutoCommit: Boolean = false
  def connectionUrl: String
  def properties: Properties
  def initializers: List[Fragment] = Nil
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
                            sshTunnel: Option[TunnelConfig]) extends StorageTarget {
    def driver: String = "com.amazon.redshift.jdbc42.Driver"

    def connectionUrl: String = s"jdbc:redshift://$host:$port/$database"

    def properties: Properties = {
      val props = new Properties()
      jdbc.validation match {
        case Right(updaters) =>
          updaters.foreach(f => f(props))
        case Left(error) =>
          throw new IllegalStateException(s"Redshift JDBC properties are invalid. ${error.message}")
      }
      props
    }
  }

  final case class Databricks(
                               host: String,
                               catalog: String,
                               schema: String,
                               port: Int,
                               httpPath: String,
                               password: PasswordConfig,
                               sshTunnel: Option[TunnelConfig],
                               userAgent: String
                             ) extends StorageTarget {

    override def username: String = "token"

    override def driver: String = "com.databricks.client.jdbc.Driver"

    override def connectionUrl: String = s"jdbc:databricks://$host:$port"

    override def doobieCommitStrategy: Strategy   = Strategy.void
    override def doobieNoCommitStrategy: Strategy = Strategy.void
    override def withAutoCommit                   = true

    override def properties: Properties = {
      val props: Properties = new Properties()
      props.put("httpPath", httpPath)
      props.put("ssl", 1)
      //      props.put("LogLevel", 6)
      props.put("AuthMech", 3)
      props.put("transportMode", "http")
      props.put("UserAgentEntry", userAgent)
      props
    }

    override def initializers: List[Fragment] =
      List(
        sql"USE CATALOG ${Fragment.const0(catalog)}",
        sql"USE ${Fragment.const0(schema)}"
      )
  }

  final case class Snowflake(snowflakeRegion: String,
                             username: String,
                             role: Option[String],
                             password: PasswordConfig,
                             account: String,
                             warehouse: String,
                             database: String,
                             schema: String,
                             transformedStage: String,
                             appName: String,
                             folderMonitoringStage: Option[String],
                             maxError: Option[Int],
                             jdbcHost: Option[String]) extends StorageTarget {
    def connectionUrl: String = s"jdbc:snowflake://$host"

    def sshTunnel: Option[TunnelConfig] = None

    def properties: Properties = {
      val props: Properties = new Properties()
      props.put("account", account)
      props.put("warehouse", warehouse)
      props.put("db", database)
      props.put("schema", schema)
      props.put("application", appName)
      role.foreach(r => props.put("role", r))
      props
    }

    def driver: String = "net.snowflake.client.jdbc.SnowflakeDriver"

    def host: String = {
      // See https://docs.snowflake.com/en/user-guide/jdbc-configure.html#connection-parameters
      val AwsUsWest2Region = "us-west-2"
      // A list of AWS region names for which the Snowflake account name doesn't have the `aws` segment
      val AwsRegionsWithoutSegment = List("us-east-1", "eu-west-1", "eu-central-1", "ap-southeast-1", "ap-southeast-2")
      // A list of AWS region names for which the Snowflake account name requires the `aws` segment
      val AwsRegionsWithSegment =
        List("us-east-2", "us-east-1-gov", "ca-central-1", "eu-west-2", "ap-northeast-1", "ap-south-1")
      val GcpRegions = List("us-central1", "europe-west2", "europe-west4")
      //val AzureRegions = List("west-us-2", "central-us", "east-us-2", "us-gov-virginia", "canada-central", "west-europe", "switzerland-north", "southeast-asia", "australia-east")

      // Host corresponds to Snowflake full account name which might include cloud platform and region
      // See https://docs.snowflake.com/en/user-guide/jdbc-configure.html#connection-parameters
      jdbcHost match {
        case Some(overrideHost) => overrideHost
        case None =>
          if (snowflakeRegion == AwsUsWest2Region)
            s"${account}.snowflakecomputing.com"
          else if (AwsRegionsWithoutSegment.contains(snowflakeRegion))
            s"${account}.${snowflakeRegion}.snowflakecomputing.com"
          else if (AwsRegionsWithSegment.contains(snowflakeRegion))
            s"${account}.${snowflakeRegion}.aws.snowflakecomputing.com"
          else if (GcpRegions.contains(snowflakeRegion))
            s"${account}.${snowflakeRegion}.gcp.snowflakecomputing.com"
          else s"${account}.${snowflakeRegion}.azure.snowflakecomputing.com"
      }
    }
  }

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

  implicit def redshiftConfigDecoder: Decoder[Redshift] =
    deriveDecoder[Redshift]

  implicit def databricksConfigDecoder: Decoder[Databricks] =
    deriveDecoder[Databricks]

  implicit def snowflakeConfigDecoder: Decoder[Snowflake] =
    deriveDecoder[Snowflake]

  implicit def encryptedConfigDecoder: Decoder[EncryptedConfig] =
    deriveDecoder[EncryptedConfig]

  implicit def tunnelConfigDecoder: Decoder[TunnelConfig] =
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
        case Right("snowflake") =>
          cur.as[Snowflake]
        case Right("databricks") =>
          cur.as[Databricks]
        case Right(other) =>
          Left(DecodingFailure(s"Storage target of type $other is not supported yet", typeCur.history))
        case Left(DecodingFailure(_, List(CursorOp.DownField("type")))) =>
          Left(DecodingFailure("Cannot find 'type' string in storage configuration", typeCur.history))
        case Left(other) =>
          Left(other)
      }
    }
}
