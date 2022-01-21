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
package com.snowplowanalytics.snowplow.rdbloader.config.components

import io.circe._
import io.circe.generic.semiauto.deriveDecoder

/** A password can be either plain-text or encrypted in the EC2 Parameter Store. */
sealed trait PasswordConfig extends Product with Serializable {
  def getUnencrypted: String = this match {
    case PasswordConfig.PlainText(plain)                                  => plain
    case PasswordConfig.EncryptedKey(PasswordConfig.EncryptedConfig(key)) => key.parameterName
  }
}

object PasswordConfig {
  final case class PlainText(value: String) extends PasswordConfig
  final case class EncryptedKey(value: EncryptedConfig) extends PasswordConfig

  /** Reference to encrypted entity inside EC2 Parameter Store */
  final case class ParameterStoreConfig(parameterName: String)

  /** Reference to encrypted key (EC2 Parameter Store only) */
  final case class EncryptedConfig(ec2ParameterStore: ParameterStoreConfig)

  implicit object PasswordDecoder extends Decoder[PasswordConfig] {
    def apply(hCursor: HCursor): Decoder.Result[PasswordConfig] =
      hCursor.value.asString match {
        case Some(s) => Right(PasswordConfig.PlainText(s))
        case None =>
          hCursor.value.asObject match {
            case Some(_) => hCursor.value.as[EncryptedConfig].map(PasswordConfig.EncryptedKey)
            case None =>
              Left(
                DecodingFailure("password should be either plain text or reference to encrypted key", hCursor.history)
              )
          }
      }
  }

  implicit def encryptedConfigDecoder: Decoder[EncryptedConfig] =
    deriveDecoder[EncryptedConfig]

  implicit def parameterStoreConfigDecoder: Decoder[ParameterStoreConfig] =
    deriveDecoder[ParameterStoreConfig]
}
