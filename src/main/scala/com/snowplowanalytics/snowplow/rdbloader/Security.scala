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

import cats.Functor
import cats.implicits._

import config.StorageTarget.TunnelConfig

/** Functions working with identities and security layers */
object Security {

  /** Actual SSH identity data. Both passphrase and key are optional */
  case class Identity(passphrase: Option[Array[Byte]], key: Option[Array[Byte]])

  /** Tunnel configuration with retrieved identity, ready to be used for establishing tunnel */
  case class Tunnel(config: TunnelConfig, identity: Identity)

  private val F = Functor[Action].compose[Either[LoaderError, ?]]

  /** Convert pure tunnel configuration to configuration with actual key and passphrase */
  def getIdentity(tunnelConfig: TunnelConfig): Action[Either[LoaderError, Identity]] = {
    val key = tunnelConfig.bastion.key.map(_.ec2ParameterStore.parameterName).map(LoaderA.getEc2Property)
    // Invert Option, Either and Action
    val keyBytes: Action[Either[LoaderError, Option[Array[Byte]]]] = key.sequence.map(_.sequence.map(_.map(_.getBytes)))
    F.map(keyBytes)(key => Identity(tunnelConfig.bastion.passphrase.map(_.getBytes()), key))
  }

  /** Perform loading and make sure tunnel is closed */
  def bracket(tunnelConfig: Option[TunnelConfig], action: TargetLoading[LoaderError, Unit]): TargetLoading[LoaderError, Unit] = {
    tunnelConfig match {
      case Some(tunnel) => for {
        identity <- getIdentity(tunnel).withoutStep
        _ <- LoaderA.establishTunnel(Security.Tunnel(tunnel, identity)).withoutStep
        _ <- action
        _ <- LoaderA.closeTunnel().withoutStep
      } yield ()
      case None => action
    }
  }
}
