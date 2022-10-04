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
package com.snowplowanalytics.snowplow.rdbloader.utils

import cats.Monad
import cats.effect.{ConcurrentEffect, Resource, Sync}
import cats.syntax.all._
import cats.effect.syntax.all._
import com.jcraft.jsch.{JSch, Session, Logger => JLogger}
import com.snowplowanalytics.snowplow.rdbloader.aws.AWS
import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget.TunnelConfig
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object SSH {

  implicit def unsafeLogger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  /** Actual SSH identity data. Both passphrase and key are optional */
  case class Identity(passphrase: Option[Array[Byte]], key: Option[Array[Byte]])

  /** Open SSH tunnel, which will be guaranteed to be closed when application exits */
  def resource[F[_]:ConcurrentEffect: Sync](tunnelConfig: Option[TunnelConfig]): Resource[F, Unit] =
    tunnelConfig match {
      case Some(tunnel) =>
        Resource.eval{

          Sync[F].delay(JSch.setLogger(new JLogger{
          override def isEnabled(level: Int): Boolean = true

          override def log(level: Int, message: String): Unit = level match {
            case JLogger.INFO => Logger[F].info("JCsh: " + message).toIO.unsafeRunSync()
            case JLogger.ERROR => Logger[F].error("JCsh: " + message).toIO.unsafeRunSync()
            case JLogger.DEBUG => Logger[F].debug("JCsh: " + message).toIO.unsafeRunSync()
            case JLogger.WARN => Logger[F].warn("JCsh: " + message).toIO.unsafeRunSync()
            case JLogger.FATAL => Logger[F].error("JCsh: " + message).toIO.unsafeRunSync()
            case _ => Logger[F].warn("NO LOG LEVEL JCsh: " + message).toIO.unsafeRunSync()
          }
        }))} >>
        Resource.make(getIdentity[F](tunnel).flatMap(i => createSession(tunnel, i)))(s => Sync[F].delay(s.disconnect())).void
      case None =>
        Resource.pure[F, Unit](())
    }

  /** Convert pure tunnel configuration to configuration with actual key and passphrase */
  def getIdentity[F[_]: Monad: Sync](tunnelConfig: TunnelConfig): F[Identity] =
    tunnelConfig
      .bastion
      .key
      .map(_.ec2ParameterStore.parameterName).traverse(AWS.getEc2Property[F])
      .map { key => Identity(tunnelConfig.bastion.passphrase.map(_.getBytes), key) }

  /**
   * Create a SSH tunnel to bastion host and set port forwarding to target DB
   * @param tunnelConfig SSH-tunnel configuration
   * @return either nothing on success and error message on failure
   */
  def createSession[F[_]: Sync](tunnelConfig: TunnelConfig, identity: Identity): F[Session] =
    Sync[F].delay {
      val jsch = new JSch() {
        val _: Identity = identity
      }
      jsch.addIdentity("rdb-loader-tunnel-key", identity.key.orNull, null, identity.passphrase.orNull)
      val sshSession = jsch.getSession(tunnelConfig.bastion.user, tunnelConfig.bastion.host, tunnelConfig.bastion.port)
      sshSession.setConfig("StrictHostKeyChecking", "no")
      sshSession.connect()
      val _ = sshSession.setPortForwardingL(tunnelConfig.localPort, tunnelConfig.destination.host, tunnelConfig.destination.port)
      sshSession
    }
}

