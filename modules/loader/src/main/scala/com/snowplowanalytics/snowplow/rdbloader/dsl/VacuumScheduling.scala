package com.snowplowanalytics.snowplow.rdbloader.dsl

import fs2.Stream
import retry._
import retry.syntax.all._
import cats.syntax.all._
import cats.effect.{Concurrent, MonadThrow, Timer}
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Statement
import eu.timepit.fs2cron.ScheduledStreams
import eu.timepit.fs2cron.cron4s.Cron4sScheduler
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import scala.concurrent.duration._

object VacuumScheduling {

  def retryPolicy[F[_]: Timer: Concurrent]: RetryPolicy[F] =
    RetryPolicies.fibonacciBackoff[F](1.minute) join RetryPolicies.limitRetries[F](10)

  def logError[F[_]: Logging](err: Throwable, details: RetryDetails): F[Unit] = details match {

    case WillDelayAndRetry(
      nextDelay: FiniteDuration,
      retriesSoFar: Int,
      cumulativeDelay: FiniteDuration) => Logging[F].warning(
      s"Failed to vacuum with ${err.getMessage}. So far we have retried $retriesSoFar times over for $cumulativeDelay. Next attempt in $nextDelay.")


    case GivingUp(totalRetries: Int, totalDelay: FiniteDuration) =>
      Logging[F].error(
        s"Failed to vacuum with ${err.getMessage}. Giving up after $totalRetries retries after $totalDelay."
      )
  }

  def run[F[_]: Transaction[*[_], C]: Concurrent: Logging: Timer, C[_]: DAO: MonadThrow: Logging](tgt: StorageTarget,cfg: Config.Schedules): Stream[F, Unit] =  {
    val vacuumEvents: Stream[F, Unit] = tgt match {
      case _: StorageTarget.Databricks => cfg.optimizeEvents match {
        case Some(cron) =>
          new ScheduledStreams(Cron4sScheduler.systemDefault[F])
            .awakeEvery(cron)
            .evalMap { _ =>
              Transaction[F, C].transact(
                Logging[C].info("initiating events vacuum") *> DAO[C].executeQuery(Statement.VacuumEvents) *> Logging[C].info("vacuum events complete")
              ).retryingOnAllErrors(retryPolicy[F], logError[F]).orElse(().pure[F])
            }
        case _ => Stream.empty[F]
      }
      case _ => Stream.empty[F]
    }

    val vacuumManifest: Stream[F, Unit]  = tgt match {
        case _: StorageTarget.Databricks => cfg.optimizeManifest match {
          case Some(cron) =>
            new ScheduledStreams(Cron4sScheduler.systemDefault[F])
              .awakeEvery(cron)
              .evalMap { _ =>
                Transaction[F, C].transact(
                  Logging[C].info("initiating manifest vacuum") *> DAO[C].executeQuery(Statement.VacuumManifest) *> Logging[C].info("vacuum manifest complete")
                ).retryingOnAllErrors(retryPolicy[F], logError[F]).orElse(().pure[F])
        }
           case _ => Stream.empty[F]
        }
        case _ => Stream.empty[F]
      }

    vacuumEvents merge vacuumManifest
  }
}
