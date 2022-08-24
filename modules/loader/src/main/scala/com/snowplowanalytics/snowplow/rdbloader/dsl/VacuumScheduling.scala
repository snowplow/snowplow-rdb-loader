package com.snowplowanalytics.snowplow.rdbloader.dsl

import fs2.Stream
import cats.syntax.all._
import cats.effect.{Concurrent, MonadThrow, Timer}
import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget
import com.snowplowanalytics.snowplow.rdbloader.db.Statement
import eu.timepit.fs2cron.ScheduledStreams
import eu.timepit.fs2cron.cron4s.Cron4sScheduler

object VacuumScheduling {

  def run[F[_]: Transaction[*[_], C]: Concurrent: Timer, C[_]: DAO: MonadThrow: Logging](cfg: StorageTarget): Stream[F, Unit] =  {
    val vacuumEvents: Stream[F, Unit] = cfg match {
      case c: StorageTarget.Databricks => c.analyzeEventsSchedule match {
        case Some(cron) =>
          new ScheduledStreams(Cron4sScheduler.systemDefault[F])
            .awakeEvery(cron)
            .evalMap { _ =>
              Transaction[F, C].transact(
                Logging[C].info("initiating events vacuum") *> DAO[C].executeQuery(Statement.VacuumEvents) *> Logging[C].info("vacuum events complete")
              ) }
        case _ => Stream.empty[F]
      }
      case _ => Stream.empty[F]
    }

    val vacuumManifest: Stream[F, Unit]  = cfg match {
        case c: StorageTarget.Databricks => c.analyzeManifestSchedule match {
          case Some(cron) =>
            new ScheduledStreams(Cron4sScheduler.systemDefault[F])
              .awakeEvery(cron)
              .evalMap { _ =>
                Transaction[F, C].transact(
                  Logging[C].info("initiating manifest vacuum") *> DAO[C].executeQuery(Statement.VacuumManifest) *> Logging[C].info("vacuum manifest complete")
                )}
           case _ => Stream.empty[F]
        }
        case _ => Stream.empty[F]
      }

    vacuumEvents merge vacuumManifest
  }
}
