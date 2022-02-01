package com.snowplowanalytics.snowplow.rdbloader.test
import cats.syntax.all._
import cats.effect.Resource
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.loading.{Load, Stage}
import com.snowplowanalytics.snowplow.rdbloader.state.{Control, State}

import java.time.Instant

object PureControl {
  def interpreter: Control[Pure] =
    new Control[Pure] {

      override def incrementLoaded: Pure[Unit] = Pure.log("Control incrementLoaded")

      override def incrementAttempts: Pure[Unit] = Pure.log("Control incrementAttempts")

      override def getAndResetAttempts: Pure[Int] = Pure.log("Control getAndResetAttempts").as(0)

      override def get: Pure[State] = Pure.pure(State(Load.Status.Idle, Instant.MIN, 0, Map.empty, 0, 0))

      override def setStage(stage: Stage): Pure[Unit] = Pure.log(s"setStage ${stage.show}")

      override def addFailure(config: Option[Config.RetryQueue])(base: Folder)(error: Throwable): Pure[Boolean] =
        Pure.log(s"Control addFailure $base ${error.getMessage}").as(true)

      override def makePaused(who: String): Resource[Pure, Unit] =
        Resource.eval(Pure.log(s"Control makePaused $who"))

      override def makeBusy(folder: Folder): Resource[Pure, Unit] =
        Resource.eval(Pure.log(s"Control makeBusy $folder"))

      override def incrementMessages: Pure[State] = Pure.pure(State(Load.Status.Idle, Instant.MIN, 0, Map.empty, 0, 0))
    }

}
