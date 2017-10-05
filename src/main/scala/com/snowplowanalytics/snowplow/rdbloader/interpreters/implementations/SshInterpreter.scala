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
package com.snowplowanalytics.snowplow.rdbloader.interpreters.implementations

import scala.util.control.NonFatal

import cats.implicits._

import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest

import com.jcraft.jsch.{JSch, Session}

import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.Security.Tunnel

/** Real-world interpreter, responsible for maintaining **single** SSH session for tunnel */
object SshInterpreter {

  private val jsch = new JSch()
  private var sshSession: Session = _

  /**
    * Create a SSH tunnel to bastion host and set port forwarding to target DB
    * @param tunnel SSH-tunnel configuration
    * @return either nothing on success and error message on failure
    */
  def establishTunnel(tunnel: Tunnel): Either[LoaderError, Unit] = {
    if (sshSession != null) Left(LoaderError.LoaderLocalError("Session for SSH tunnel already opened"))
    else {
      val either = Either.catchNonFatal {
        jsch.addIdentity("rdb-loader-tunnel-key", tunnel.identity.key.orNull, null, tunnel.identity.passphrase.orNull)
        sshSession = jsch.getSession(tunnel.config.bastion.user, tunnel.config.bastion.host, tunnel.config.bastion.port)
        sshSession.setConfig("StrictHostKeyChecking", "no")
        sshSession.connect()
        val _ = sshSession.setPortForwardingL(tunnel.config.localPort, tunnel.config.destination.host, tunnel.config.destination.port)
        ()
      }
      either.leftMap(e => LoaderError.LoaderLocalError(e.getMessage))
    }
  }

  /** Try to close SSH tunnel, fail if it was not open */
  def closeTunnel(): Either[LoaderError, Unit] =
    if (sshSession == null) Left(LoaderError.LoaderLocalError("Attempted to close nonexistent SSH session"))
    else try {
      Right(sshSession.disconnect())
    } catch {
      case NonFatal(e) => Left(LoaderError.LoaderLocalError(e.getMessage))
    }

  /**
    * Get value from AWS EC2 Parameter Store
    * @param name systems manager parameter's name with SSH key
    * @return decrypted string with key
    */
  def getKey(name: String): Either[LoaderError, String] = {
    try {
      val client = AWSSimpleSystemsManagementClientBuilder.defaultClient()
      val req: GetParameterRequest = new GetParameterRequest().withName(name).withWithDecryption(true)
      val par = client.getParameter(req)
      Right(par.getParameter.getValue)
    } catch {
      case NonFatal(e) => Left(LoaderError.LoaderLocalError(e.getMessage))
    }
  }
}
