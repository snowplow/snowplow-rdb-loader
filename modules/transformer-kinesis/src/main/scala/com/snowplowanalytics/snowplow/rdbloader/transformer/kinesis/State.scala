package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis

import cats.implicits._
import cats.kernel.Monoid
import cats.Show
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.Processing.{SuccessfulTransformation, TransformationResult}

import java.time.Instant

case class State(total: Long,
                 bad: Long,
                 minCollector: Option[Instant],
                 maxCollector: Option[Instant],
                 types: Set[Data.ShreddedType]) {
  def show: String =
    List(
      s"total=$total",
      s"bad=$bad",
      s"minCollector=${minCollector.map(_.toString).getOrElse("null")}",
      s"maxCollector=${maxCollector.map(_.toString).getOrElse("null")}",
      s"types=[${types.toList.map(_.schemaKey.toSchemaUri).mkString(",")}]"
    ).mkString(";")
}

object State {

  def fromEvent(row: TransformationResult): State =
    row match {
      case Left(_) =>
        Monoid[State].empty.copy(total = 1, bad = 1)
      case Right(SuccessfulTransformation(event, _)) =>
        State(1, 0, Some(event.collector_tstamp), Some(event.collector_tstamp), event.inventory)
    }

  private def combineOption[A](f: (A, A) => A, a: Option[A], b: Option[A]): Option[A] =
    (a, b).mapN(f).orElse(a).orElse(b)

  implicit val stateMonoid: Monoid[State] =
    new Monoid[State] {
      def empty: State = State(0L, 0L, None, None, Set.empty)

      def combine(x: State, y: State): State =
        State(
          x.total + y.total,
          x.bad + y.bad,
          combineOption(Ordering[Instant].min, x.minCollector, y.minCollector),
          combineOption(Ordering[Instant].max, x.maxCollector, y.maxCollector),
          x.types ++ y.types
        )
    }

  implicit val stateShow: Show[State] =
    Show.show(_.show)
}

