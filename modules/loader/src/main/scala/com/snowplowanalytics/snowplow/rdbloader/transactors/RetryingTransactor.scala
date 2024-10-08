/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader.transactors

import cats.effect.{Clock, Resource}
import cats.effect.kernel.{MonadCancelThrow, Temporal}
import cats.syntax.all._
import doobie.Transactor
import retry._

import java.sql.Connection
import java.sql.{SQLException, SQLTransientConnectionException}

import com.snowplowanalytics.snowplow.rdbloader.UnskippableException
import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import com.snowplowanalytics.snowplow.rdbloader.loading.Retry
import com.snowplowanalytics.snowplow.rdbloader.loading.Retry._

object RetryingTransactor {

  /**
   * Exception for when we received exceptions which should be transient, but they continued beyond
   * our retry limit
   */
  class ExceededRetriesException(cause: Throwable)
      extends Exception("Failed to establish a JDBC connection", cause)
      with UnskippableException

  /**
   * Exception for when we could not get a healthy connection
   *
   * It is tagged as UnskippableException because loading of any batch would be impacted by this
   * same exception.
   */
  class UnskippableConnectionException(cause: Throwable)
      extends Exception("Failed to establish a JDBC connection", cause)
      with UnskippableException

  object ExceededRetriesException {

    /**
     * Extractor that matches if the exception, or any underlying cause of the exception, was for
     * exceeded retries
     */
    def unapply(t: Throwable): Boolean =
      (t, Option(t.getCause)) match {
        case (_: ExceededRetriesException, _) => true
        case (_, Some(cause))                 => unapply(cause)
        case (_, None)                        => false
      }
  }

  /**
   * A doobie transactor that retries getting a connection if the HikariPool times out. It blocks
   * the application until the connection is available, or until the retry limits are exceeded.
   */
  def wrap[F[_]: Temporal: Logging: Sleep, A](
    config: Config.Retries,
    inner: Transactor.Aux[F, A]
  ): Transactor.Aux[F, A] =
    inner.copy(connect0 = a => wrapResource(config, inner.connect(a)))

  private def wrapResource[F[_]](
    config: Config.Retries,
    resource: Resource[F, Connection]
  )(implicit F: MonadCancelThrow[F],
    L: Logging[F],
    S: Sleep[Resource[F, *]],
    C: Clock[Resource[F, *]]
  ): Resource[F, Connection] =
    Retry
      .retryingOnSomeErrors(config, isTransientError.andThen(_.pure[Resource[F, *]]), onError[F](_, _), resource)
      .adaptError {
        case t if isTransientError(t) => new ExceededRetriesException(t)
        case t: Throwable             => new UnskippableConnectionException(t)
      }

  /**
   * Matches against the exception against the recognizable signatures which tell us the Hikari pool
   * could not get any connection within a time limit. For Databricks, it likely tells us the
   * cluster is still starting up.
   */
  private val isTransientError: Throwable => Boolean = {
    case e: SQLTransientConnectionException =>
      Option(e.getCause) match {
        case None =>
          // Expected when:
          // - The HikariPool has only just started up, and has not yet made any connection
          // - ...and the JDBC server (e.g. Databricks cluster) times out on a connection
          true
        case Some(cause: SQLException) =>
          Option(cause.getSQLState) match {
            case Some("HYT01") =>
              // Expected when:
              // - The HikariPool has already received 1+ connection timeouts from the JDBC server
              // - ...and the JDBC server (e.g. Databricks cluster) times out on follow-up attempts to get a connection
              true
            case _ =>
              // Expected when the JDBC driver cannot connect to the server for any other reason,
              // e.g. authorization failure.
              // We don't want to retry these exceptions, so return false
              false
          }
        case _ =>
          false
      }
    case _: Throwable =>
      false
  }

  private def onError[F[_]: Logging](t: Throwable, d: RetryDetails): Resource[F, Unit] =
    Resource.eval(Logging[F].info(show"Target is not ready. $d. ${t.getMessage}"))
}
