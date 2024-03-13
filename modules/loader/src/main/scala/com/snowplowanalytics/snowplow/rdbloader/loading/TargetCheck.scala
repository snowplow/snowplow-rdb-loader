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
package com.snowplowanalytics.snowplow.rdbloader.loading

import cats.MonadThrow
import cats.implicits._
import retry._

import com.snowplowanalytics.snowplow.rdbloader.UnskippableException
import com.snowplowanalytics.snowplow.rdbloader.db.Statement
import com.snowplowanalytics.snowplow.rdbloader.dsl.{DAO, Logging, Transaction}

/**
 * Module prepares the target to make it ready for loading.
 */
object TargetCheck {

  class TargetCheckException(cause: Throwable) extends Exception(cause.getMessage, cause) with UnskippableException

  /**
   * Prepare the target to make it ready for loading e.g. start the warehouse running
   */
  def prepareTarget[F[_]: MonadThrow: Transaction[*[_], C]: Logging: Sleep, C[_]: DAO]: F[Unit] =
    Transaction[F, C]
      .run(DAO[C].executeQuery[Unit](Statement.ReadyCheck))
      .void
      .adaptError { case t: Throwable =>
        // Tags the exception as unskippable
        new TargetCheckException(t)
      }
}
