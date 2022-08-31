package com.snowplowanalytics.snowplow.rdbloader.dsl

import cats.ApplicativeError
import fs2.Stream
import retry._
import retry.syntax.all._
import cats.syntax.all._
import cats.effect.{Concurrent, MonadThrow, Timer}
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Statement
import eu.timepit.fs2cron.ScheduledStreams
import eu.timepit.fs2cron.cron4s.Cron4sScheduler

import scala.concurrent.duration._

object VacuumScheduling {

  def retryPolicy[F[_]: Timer: Concurrent]: RetryPolicy[F] =
    RetryPolicies.fibonacciBackoff[F](1.minute) join RetryPolicies.limitRetries[F](10)

  def run[F[_]: Transaction[*[_], C]: Concurrent: Timer, C[_]: DAO: MonadThrow: Logging](tgt: StorageTarget,cfg: Config.Schedules): Stream[F, Unit] =  {
    val vacuumEvents: Stream[F, Unit] = tgt match {
      case _: StorageTarget.Databricks => cfg.optimizeEventsSchedule match {
        case Some(cron) =>
          new ScheduledStreams(Cron4sScheduler.systemDefault[F])
            .awakeEvery(cron)
            .evalMap { _ =>
              Transaction[F, C].transact(
                Logging[C].info("initiating events vacuum") *> DAO[C].executeQuery(Statement.VacuumEvents) *> Logging[C].info("vacuum events complete")
              ).retryingOnAllErrors(retryPolicy[F], (e,_)  => ApplicativeError[F, Throwable].raiseError(e))}
        case _ => Stream.empty[F]
      }
      case _ => Stream.empty[F]
    }

    val vacuumManifest: Stream[F, Unit]  = tgt match {
        case _: StorageTarget.Databricks => cfg.optimizeManifestSchedule match {
          case Some(cron) =>
            new ScheduledStreams(Cron4sScheduler.systemDefault[F])
              .awakeEvery(cron)
              .evalMap { _ =>
                Transaction[F, C].transact(
                  Logging[C].info("initiating manifest vacuum") *> DAO[C].executeQuery(Statement.VacuumManifest) *> Logging[C].info("vacuum manifest complete")
                ).retryingOnAllErrors(retryPolicy[F], (e,_)  => ApplicativeError[F, Throwable].raiseError(e))
        }
           case _ => Stream.empty[F]
        }
        case _ => Stream.empty[F]
      }

    vacuumEvents merge vacuumManifest
  }
}
