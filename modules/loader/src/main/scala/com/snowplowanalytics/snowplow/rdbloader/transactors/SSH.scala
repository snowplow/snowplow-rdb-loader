/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transactors

import cats.Monad
import cats.effect.{Async, Resource, Sync}
import cats.effect.kernel.{MonadCancel, Ref}
import cats.effect.std.{Dispatcher, Hotswap, Semaphore}
import cats.syntax.all._
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

  final class SSHException(cause: Throwable) extends Exception(s"Error setting up SSH tunnel", cause)

  /**
   * A doobie transactor that ensures the SSH tunnel is connected before attempting a connection to
   * the warehouse
   */
  def transactor[F[_]: Async: SecretStore, A](
    config: TunnelConfig,
    inner: Transactor.Aux[F, A]
  ): Resource[F, Transactor.Aux[F, A]] =
    for {
      dispatcher <- Dispatcher.sequential[F](await = false)
      _ <- Resource.eval(configureLogging(dispatcher))
      identity <- Resource.eval(getIdentity(config))
      hs <- Hotswap.create[F, Session]
      ref <- Resource.eval(Ref.of[F, Option[Session]](None))
      sem <- Resource.eval(Semaphore[F](1))
    } yield inner.copy(connect0 = a => Resource.eval(ensureTunnel(ref, sem, hs, connectedSession(config, identity))) *> inner.connect(a))

  /**
   * Ensure an SSH tunnel is connected.
   *
   * Uses a semaphore to prevent multiple fibers trying to connect a session at the same time
   */
  def ensureTunnel[F[_]: Sync](
    state: Ref[F, Option[Session]],
    sem: Semaphore[F],
    hs: Hotswap[F, Session],
    sessionResource: Resource[F, Session]
  ): F[Unit] =
    sem.permit.use { _ =>
      state.get.flatMap {
        case Some(session) if session.isConnected =>
          Logger[F].debug("SSH session is already connected")
        case _ =>
          hs.clear *>
            MonadCancel[F]
              .uncancelable { _ =>
                for {
                  session <- hs.swap(sessionResource)
                  _ <- state.set(Some(session))
                } yield ()
              }
              .adaptError { case t: Throwable =>
                new SSHException(t)
              }
      }
    }

  def configureLogging[F[_]: Sync](dispatcher: Dispatcher[F]): F[Unit] =
    Sync[F].delay {
      JSch.setLogger(new JLogger {
        override def isEnabled(level: Int): Boolean = true

        override def log(level: Int, message: String): Unit = dispatcher.unsafeRunSync(level match {
          case JLogger.INFO => Logger[F].info("JCsh: " + message)
          case JLogger.ERROR => Logger[F].error("JCsh: " + message)
          case JLogger.DEBUG => Logger[F].debug("JCsh: " + message)
          case JLogger.WARN => Logger[F].warn("JCsh: " + message)
          case JLogger.FATAL => Logger[F].error("JCsh: " + message)
          case _ => Logger[F].warn("NO LOG LEVEL JCsh: " + message)
        })
      })
    }

  /** Convert pure tunnel configuration to configuration with actual key and passphrase */
  def getIdentity[F[_]: Monad: Sync: SecretStore](tunnelConfig: TunnelConfig): F[Identity] =
    tunnelConfig.bastion.key
      .map(_.parameterName)
      .traverse(SecretStore[F].getValue)
      .map(key => Identity(tunnelConfig.bastion.passphrase.map(_.getBytes), key.map(_.getBytes)))

  /** It is important to tear down the port forwarding before tearing down the connection */
  def connectedSession[F[_]: Sync](
    config: TunnelConfig,
    identity: Identity
  ): Resource[F, Session] =
    for {
      _ <- Resource.eval(Logger[F].info("Creating new SSH session"))
      session <- Resource.make(createSession(config, identity))(s => Sync[F].delay(s.disconnect()))
      _ <- setPortForwarding(config, session)
      _ <- Resource.eval(Sync[F].blocking(session.connect()))
      _ <- Resource.make(Sync[F].unit)(_ => Logger[F].info("Closing SSH session"))
    } yield session

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
    val acquire = Sync[F].delay {
      session.setPortForwardingL(config.localPort, config.destination.host, config.destination.port)
    }.void
    val release = Sync[F].delay {
      session.delPortForwardingL(config.localPort)
    }
    Resource.make(acquire)(_ => release)
  }
}
