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

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

/**
  * SSH configuration, allowing target to be loaded through tunnel
  *
  * @param bastion Bastion host SSH configuration
  * @param localPort local port to which RDB Loader should connect,
  *                  same port as in `StorageTarget`, can be arbitrary
  * @param destination end-socket of SSH tunnel (host/port pair to access DB)
  */
final case class TunnelConfig(
  bastion: TunnelConfig.BastionConfig,
  localPort: Int,
  destination: TunnelConfig.DestinationConfig
)

object TunnelConfig {

  /** Bastion host access configuration for SSH tunnel */
  final case class BastionConfig(
    host: String,
    port: Int,
    user: String,
    passphrase: Option[String],
    key: Option[PasswordConfig.EncryptedConfig]
  )

  /** Destination socket for SSH tunnel - usually DB socket inside private network */
  final case class DestinationConfig(host: String, port: Int)

  implicit def tunnerConfigDecoder: Decoder[TunnelConfig] =
    deriveDecoder[TunnelConfig]

  implicit def bastionConfigDecoder: Decoder[BastionConfig] =
    deriveDecoder[BastionConfig]

  implicit def destinationConfigDecoder: Decoder[DestinationConfig] =
    deriveDecoder[DestinationConfig]
}
