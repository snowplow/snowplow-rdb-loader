/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
