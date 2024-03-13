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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import fs2.Stream
import cats.syntax.all._
import cats.MonadThrow
import cats.effect.kernel.Async
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Statement
import eu.timepit.fs2cron.cron4s.Cron4sScheduler

object VacuumScheduling {

  def run[F[_]: Transaction[*[_], C]: Async: Logging, C[_]: DAO: MonadThrow: Logging](
    tgt: StorageTarget,
    cfg: Config.Schedules
  ): Stream[F, Unit] = {
    val vacuumEvents: Stream[F, Unit] = tgt match {
      case _: StorageTarget.Databricks =>
        cfg.optimizeEvents match {
          case Some(cron) =>
            Cron4sScheduler
              .systemDefault[F]
              .awakeEvery(cron)
              .evalMap { _ =>
                Transaction[F, C]
                  .transact(
                    Logging[C].info("initiating events vacuum") *> DAO[C].executeQuery(Statement.VacuumEvents) *> Logging[C]
                      .info("vacuum events complete")
                  )
                  .recoverWith { case t: Throwable =>
                    Logging[F].error(t)("Failed to vacuum events table")
                  }
              }
          case _ => Stream.empty[F]
        }
      case _ => Stream.empty[F]
    }

    val vacuumManifest: Stream[F, Unit] = tgt match {
      case _: StorageTarget.Databricks =>
        cfg.optimizeManifest match {
          case Some(cron) =>
            Cron4sScheduler
              .systemDefault[F]
              .awakeEvery(cron)
              .evalMap { _ =>
                Transaction[F, C]
                  .transact(
                    Logging[C].info("initiating manifest vacuum") *> DAO[C].executeQuery(Statement.VacuumManifest) *> Logging[C]
                      .info("vacuum manifest complete")
                  )
                  .recoverWith { case t: Throwable =>
                    Logging[F].error(t)("Failed to vacuum manifest table")
                  }
              }
          case _ => Stream.empty[F]
        }
      case _ => Stream.empty[F]
    }

    vacuumEvents merge vacuumManifest
  }
}
