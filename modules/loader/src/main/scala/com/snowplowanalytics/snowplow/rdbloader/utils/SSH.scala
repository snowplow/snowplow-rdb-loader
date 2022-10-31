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
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Effect, Resource, Sync}
import cats.effect.concurrent.Semaphore
import cats.syntax.all._
import cats.effect.syntax.all._
import doobie.Transactor
import com.jcraft.jsch.{JSch, Logger => JLogger, Session}
import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget.TunnelConfig
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.SecretStore

object SSH {

  implicit def unsafeLogger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  /** Actual SSH identity data. Both passphrase and key are optional */
  case class Identity(passphrase: Option[Array[Byte]], key: Option[Array[Byte]])

  final class SSHException(cause: Throwable) extends Exception(s"Error setting up SSH tunnel: ${cause.getMessage}", cause)

  /**
   * A doobie transactor that ensures the SSH tunnel is connected before attempting a connection to
   * the warehouse
   */
  def transactor[F[_]: ConcurrentEffect: ContextShift: SecretStore, A](
    config: TunnelConfig,
    blocker: Blocker,
    inner: Transactor.Aux[F, A]
  ): Resource[F, Transactor.Aux[F, A]] =
    for {
      _ <- Resource.eval(configureLogging)
      identity <- Resource.eval(getIdentity(config))
      session <- Resource.make(createSession(config, identity))(s => Sync[F].delay(s.disconnect()))
      _ <- setPortForwarding(config, session)
      sem <- Resource.eval(Semaphore[F](1))
    } yield inner.copy(connect0 = a => Resource.eval(ensureTunnel(session, blocker, sem)) *> inner.connect(a))

  /**
   * Ensure the SSH tunnel is connected.
   *
   * Uses a semaphore to prevent multiple fibers trying to connect the session at the same time
   */
  def ensureTunnel[F[_]: Sync: ContextShift](
    session: Session,
    blocker: Blocker,
    sem: Semaphore[F]
  ): F[Unit] =
    sem.withPermit {
      Sync[F]
        .delay(session.isConnected())
        .ifM(
          Logger[F].debug("SSH session is already connected"),
          blocker.delay(session.connect())
        )
        .adaptError { case t: Throwable =>
          new SSHException(t)
        }
    }

  def configureLogging[F[_]: Effect]: F[Unit] =
    Sync[F].delay(JSch.setLogger(new JLogger {
      override def isEnabled(level: Int): Boolean = true

      override def log(level: Int, message: String): Unit = level match {
        case JLogger.INFO => Logger[F].info("JCsh: " + message).toIO.unsafeRunSync()
        case JLogger.ERROR => Logger[F].error("JCsh: " + message).toIO.unsafeRunSync()
        case JLogger.DEBUG => Logger[F].debug("JCsh: " + message).toIO.unsafeRunSync()
        case JLogger.WARN => Logger[F].warn("JCsh: " + message).toIO.unsafeRunSync()
        case JLogger.FATAL => Logger[F].error("JCsh: " + message).toIO.unsafeRunSync()
        case _ => Logger[F].warn("NO LOG LEVEL JCsh: " + message).toIO.unsafeRunSync()
      }
    }))

  /** Convert pure tunnel configuration to configuration with actual key and passphrase */
  def getIdentity[F[_]: Monad: Sync: SecretStore](tunnelConfig: TunnelConfig): F[Identity] =
    tunnelConfig.bastion.key
      .map(_.parameterName)
      .traverse(SecretStore[F].getValue)
      .map(key => Identity(tunnelConfig.bastion.passphrase.map(_.getBytes), key.map(_.getBytes)))

  /**
   * Create a SSH session configured for the bastion host.
   *
   * The returned session is not yet connected and is not yet listening on a local port.
   *
   * @param tunnelConfig
   *   SSH-tunnel configuration
   * @param identity
   *   SSH identity data
   */
  def createSession[F[_]: Sync](tunnelConfig: TunnelConfig, identity: Identity): F[Session] =
    Sync[F].delay {
      val jsch = new JSch() {
        val _: Identity = identity
      }
      jsch.addIdentity("rdb-loader-tunnel-key", identity.key.orNull, null, identity.passphrase.orNull)
      val sshSession = jsch.getSession(tunnelConfig.bastion.user, tunnelConfig.bastion.host, tunnelConfig.bastion.port)
      sshSession.setConfig("StrictHostKeyChecking", "no")
      sshSession
    }

  /**
   * Start the Session listening on the local port
   */
  def setPortForwarding[F[_]: Sync](config: TunnelConfig, session: Session): Resource[F, Unit] = {
    val acquire = Sync[F]
      .delay {
        session.setPortForwardingL(config.localPort, config.destination.host, config.destination.port)
      }
      .adaptError { case t: Throwable =>
        new SSHException(t)
      }
      .void
    val release = Sync[F].delay {
      session.delPortForwardingL(config.localPort)
    }
    Resource.make(acquire)(_ => release)
  }
}
