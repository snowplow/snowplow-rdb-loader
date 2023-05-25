/*
 * Copyright (c) 2014-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.cloud.authservice

import java.time.Instant

import cats.effect._
import cats.implicits._
import cats.{Applicative, ~>}

import scala.concurrent.duration.FiniteDuration

trait LoadAuthService[F[_]] { self =>
  def forLoadingEvents: F[LoadAuthService.LoadAuthMethod]
  def forFolderMonitoring: F[LoadAuthService.LoadAuthMethod]

  def mapK[G[_]](arrow: F ~> G): LoadAuthService[G] =
    new LoadAuthService[G] {
      def forLoadingEvents: G[LoadAuthService.LoadAuthMethod] = arrow(self.forLoadingEvents)
      def forFolderMonitoring: G[LoadAuthService.LoadAuthMethod] = arrow(self.forFolderMonitoring)
    }
}

object LoadAuthService {
  def apply[F[_]](implicit ev: LoadAuthService[F]): LoadAuthService[F] = ev

  /**
   * Auth method that is used with COPY INTO statement
   */
  sealed trait LoadAuthMethod

  object LoadAuthMethod {

    /**
     * Specifies auth method that doesn't use credentials Destination should be already configured
     * with some other mean for copying from transformer output bucket
     */
    final case object NoCreds extends LoadAuthMethod

    /**
     * Specifies auth method that pass temporary credentials to COPY INTO statement
     */
    sealed trait TempCreds extends LoadAuthMethod {
      def expires: Instant
    }

    object TempCreds {

      final case class AWS(
        awsAccessKey: String,
        awsSecretKey: String,
        awsSessionToken: String,
        expires: Instant
      ) extends TempCreds

      final case class Azure(
        sasToken: String,
        expires: Instant
      ) extends TempCreds
    }
  }

  trait LoadAuthMethodProvider[F[_]] {
    def get: F[LoadAuthService.LoadAuthMethod]
  }

  object LoadAuthMethodProvider {
    def noop[F[_]: Concurrent]: F[LoadAuthMethodProvider[F]] =
      Concurrent[F].pure {
        new LoadAuthMethodProvider[F] {
          def get: F[LoadAuthService.LoadAuthMethod] = Concurrent[F].pure(LoadAuthMethod.NoCreds)
        }
      }
  }

  def create[F[_]: Async](
    eventsAuthProvider: F[LoadAuthMethodProvider[F]],
    foldersAuthProvider: F[LoadAuthMethodProvider[F]]
  ): Resource[F, LoadAuthService[F]] =
    Resource.eval(
      for {
        e <- eventsAuthProvider
        f <- foldersAuthProvider
      } yield new LoadAuthService[F] {
        override def forLoadingEvents: F[LoadAuthMethod] = e.get
        override def forFolderMonitoring: F[LoadAuthMethod] = f.get
      }
    )

  def noop[F[_]: Applicative]: Resource[F, LoadAuthService[F]] =
    Resource.pure[F, LoadAuthService[F]](new LoadAuthService[F] {
      override def forLoadingEvents: F[LoadAuthMethod] =
        Applicative[F].pure(LoadAuthMethod.NoCreds)
      override def forFolderMonitoring: F[LoadAuthMethod] =
        Applicative[F].pure(LoadAuthMethod.NoCreds)
    })

  /**
   * Either fetches new temporary credentials, or returns cached temporary credentials if they are
   * still valid
   *
   * The new credentials are valid for *twice* the length of time they requested for. This means
   * there is a high chance we can re-use the cached credentials later.
   */
  def credsCache[F[_]: Async](
    credentialsTtl: FiniteDuration,
    getCreds: => F[LoadAuthMethod.TempCreds]
  ): F[LoadAuthMethodProvider[F]] =
    for {
      ref <- Ref.of(Option.empty[LoadAuthMethod.TempCreds])
    } yield new LoadAuthMethodProvider[F] {
      override def get: F[LoadAuthMethod] =
        for {
          opt <- ref.get
          now <- Clock[F].realTimeInstant
          next <- opt match {
                    case Some(tc) if tc.expires.isAfter(now.plusMillis(credentialsTtl.toMillis)) =>
                      Concurrent[F].pure(tc)
                    case _ => getCreds
                  }
          _ <- ref.set(Some(next))
        } yield next
    }
}
