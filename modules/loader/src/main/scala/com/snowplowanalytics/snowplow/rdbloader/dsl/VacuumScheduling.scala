package com.snowplowanalytics.snowplow.rdbloader.dsl

import fs2.Stream
import cats.syntax.all._
import cats.MonadThrow
import cats.effect.kernel.Async
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.{ManagedTransaction, Statement}
import eu.timepit.fs2cron.cron4s.Cron4sScheduler

import scala.concurrent.duration._

object VacuumScheduling {

  private val retryPolicy: Config.Retries =
    Config.Retries(Config.Strategy.Fibonacci, Some(10), 1.minute, None)

  def run[F[_]: Transaction[*[_], C]: Async: Logging, C[_]: DAO: MonadThrow: Logging](
    tgt: StorageTarget,
    cfg: Config.Schedules,
    readyCheckConfig: Config.Retries
  ): Stream[F, Unit] = {

    val txnConfig = ManagedTransaction.TxnConfig(readyCheck = readyCheckConfig, execution = retryPolicy)
    val vacuumEvents: Stream[F, Unit] = tgt match {
      case _: StorageTarget.Databricks =>
        cfg.optimizeEvents match {
          case Some(cron) =>
            Cron4sScheduler
              .systemDefault[F]
              .awakeEvery(cron)
              .evalMap { _ =>
                ManagedTransaction
                  .transact[F, C](txnConfig, "vacuum events") {
                    Logging[C].info("initiating events vacuum") *> DAO[C].executeQuery(Statement.VacuumEvents) *> Logging[C]
                      .info("vacuum events complete")
                  }
                  .recoverWith { case t: Throwable =>
                    Logging[F].logThrowable("Failed to vacuum events", t, Logging.Intention.CatchAndRecover)
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
                ManagedTransaction
                  .transact[F, C](txnConfig, "vacuum manifest") {
                    Logging[C].info("initiating manifest vacuum") *> DAO[C].executeQuery(Statement.VacuumManifest) *> Logging[C]
                      .info("vacuum manifest complete")
                  }
                  .recoverWith { case t: Throwable =>
                    Logging[F].logThrowable("Failed to vacuum events", t, Logging.Intention.CatchAndRecover)
                  }
              }
          case _ => Stream.empty[F]
        }
      case _ => Stream.empty[F]
    }

    vacuumEvents merge vacuumManifest
  }
}
