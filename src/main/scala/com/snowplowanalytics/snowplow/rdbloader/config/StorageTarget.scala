/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader
package config

import cats.data._
import cats.implicits._

import io.circe.{Decoder, DecodingFailure, HCursor, Json}
import io.circe.Decoder._
import io.circe.generic.auto._

import org.json4s.JValue

import com.github.fge.jsonschema.core.report.ProcessingMessage

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.validation.ValidatableJValue._

// This project
import LoaderError._
import utils.Compat._
import utils.Common._


/**
 * Common configuration for JDBC target, such as Redshift and Postgres
 * Any of those can be safely coerced
 */
sealed trait StorageTarget extends Product with Serializable {
  def name: String
  def host: String
  def database: String
  def schema: String
  def port: Int
  def sslMode: StorageTarget.SslMode
  def username: String
  def password: StorageTarget.PasswordConfig

  def eventsTable =
    loaders.Common.getEventsTable(schema)

  def shreddedTable(tableName: String) =
    s"$schema.$tableName"

  def purpose: StorageTarget.Purpose

  def sshTunnel: Option[StorageTarget.TunnelConfig]
}

object StorageTarget {

  sealed trait SslMode extends StringEnum { def asProperty = asString.toLowerCase.replace('_', '-') }
  case object Disable extends SslMode { def asString = "DISABLE" }
  case object Require extends SslMode { def asString = "REQUIRE" }
  case object VerifyCa extends SslMode { def asString = "VERIFY_CA" }
  case object VerifyFull extends SslMode { def asString = "VERIFY_FULL" }

  sealed trait Purpose extends StringEnum
  case object DuplicateTracking extends Purpose { def asString = "DUPLICATE_TRACKING" }
  case object FailedEvents extends Purpose { def asString = "FAILED_EVENTS" }
  case object EnrichedEvents extends Purpose { def asString = "ENRICHED_EVENTS" }

  implicit val sslModeDecoder =
    decodeStringEnum[SslMode]

  implicit val purposeDecoder =
    decodeStringEnum[Purpose]

  /**
   * Redshift config
   * `com.snowplowanalytics.snowplow.storage/postgresql_config/jsonschema/1-1-0`
   */
  case class PostgresqlConfig(
      id: Option[String],
      name: String,
      host: String,
      database: String,
      port: Int,
      sslMode: SslMode,
      schema: String,
      username: String,
      password: PasswordConfig,
      sshTunnel: Option[TunnelConfig])
    extends StorageTarget {
    val purpose = EnrichedEvents
  }

  /**
   * Redshift config
   * `com.snowplowanalytics.snowplow.storage/redshift_config/jsonschema/2-1-0`
   */
  case class RedshiftConfig(
      id: Option[String],
      name: String,
      host: String,
      database: String,
      port: Int,
      sslMode: SslMode,
      roleArn: String,
      schema: String,
      username: String,
      password: PasswordConfig,
      maxError: Int,
      compRows: Long,
      sshTunnel: Option[TunnelConfig])
    extends StorageTarget {
    val purpose = EnrichedEvents
  }

  /** Reference to encrypted entity inside EC2 Parameter Store */
  case class ParameterStoreConfig(parameterName: String)

  /** Reference to encrypted key (EC2 Parameter Store only so far) */
  case class EncryptedConfig(ec2ParameterStore: ParameterStoreConfig)

  /** Bastion host access configuration for SSH tunnel */
  case class BastionConfig(host: String, port: Int, user: String, passphrase: Option[String], key: Option[EncryptedConfig])

  /** Destination socket for SSH tunnel - usually DB socket inside private network */
  case class DestinationConfig(host: String, port: Int)

  /** ADT representing fact that password can be either plain-text or encrypted in EC2 Parameter Store */
  sealed trait PasswordConfig {
    def getUnencrypted: String = this match {
      case PlainText(plain) => plain
      case EncryptedKey(EncryptedConfig(key)) => key.parameterName
    }
  }
  case class PlainText(value: String) extends PasswordConfig
  case class EncryptedKey(value: EncryptedConfig) extends PasswordConfig

  /**
    * SSH configuration, enabling target to be loaded though tunnel
    *
    * @param bastion bastion host SSH configuration
    * @param localPort local port to which RDB Loader should connect,
    *                  same port as in `StorageTarget`, can be arbitrary
    * @param destination end-socket of SSH tunnel (host/port pair to access DB)
    */
  case class TunnelConfig(bastion: BastionConfig, localPort: Int, destination: DestinationConfig)

  implicit object PasswordDecoder extends Decoder[PasswordConfig] {
    def apply(hCursor: HCursor): Decoder.Result[PasswordConfig] = {
      hCursor.value.asString match {
        case Some(s) => Right(PlainText(s))
        case None => hCursor.value.asObject match {
          case Some(_) => hCursor.value.as[EncryptedConfig].map(EncryptedKey)
          case None => Left(DecodingFailure("password should be either plain text or reference to encrypted key", hCursor.history))
        }
      }
    }
  }

  /**
    * Decode Json as one of known storage targets
    *
    * @param validJson JSON that is presumably self-describing storage target configuration
    * @return validated entity of `StorageTarget` ADT if success
    */
  def decodeStorageTarget(validJson: Json): Either[DecodingError, StorageTarget] = {
    val nameDataPair = for {
      jsonObject <- validJson.asObject
      schema     <- jsonObject.toMap.get("schema")
      data       <- jsonObject.toMap.get("data")
      schemaKey  <- schema.asString
      key        <- SchemaKey.fromUri(schemaKey)
    } yield (key.name, data)

    nameDataPair match {
      case Some(("redshift_config", data)) => data.as[RedshiftConfig].leftMap(e => DecodingError(e.getMessage()))
      case Some(("postgresql_config", data)) => data.as[PostgresqlConfig].leftMap(e => DecodingError(e.getMessage()))
      case Some((name, _)) => DecodingError(s"Unsupported storage target [$name]").asLeft
      case None => DecodingError("Not a self-describing JSON was used as storage target configuration").asLeft
    }
  }

  /**
    * Parse string as `StorageTarget` validating it via Iglu resolver
    *
    * @param resolver Iglu resolver
    * @param target string presumably containing self-describing JSON with storage target
    * @return valid `StorageTarget` OR
    *         non-empty list of errors (such as validation or parse errors)
    */
  def parseTarget(resolver: Resolver, target: String): ValidatedNel[ConfigError, StorageTarget] = {
    val json = safeParse(target).toValidatedNel
    val validatedJson = json.andThen(validate(resolver))
    validatedJson.andThen(decodeStorageTarget(_).toValidatedNel)
  }

  /**
    * Validate json4s JValue AST with Iglu Resolver and immediately convert it into circe AST
    *
    * @param resolver Iglu Resolver object
    * @param json json4s AST
    * @return circe AST
    */
  private def validate(resolver: Resolver)(json: JValue): ValidatedNel[ConfigError, Json] = {
    val result: ValidatedNel[ProcessingMessage, JValue] = json.validate(dataOnly = false)(resolver)
    result.map(jvalueToCirce).leftMapNel(e => ValidationError(e.toString))  // Convert from Iglu client's format, TODO compat
  }
}
