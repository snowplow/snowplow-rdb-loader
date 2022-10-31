package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import cats.implicits._
import cats.{Monoid, Show}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Processing.{SuccessfulTransformation, TransformationResult}

import java.time.Instant

case class State[C](
  total: Long,
  bad: Long,
  minCollector: Option[Instant],
  maxCollector: Option[Instant],
  types: Set[Data.ShreddedType],
  checkpointer: C
) {
  def show: String =
    List(
      s"total=$total",
      s"bad=$bad",
      s"minCollector=${minCollector.map(_.toString).getOrElse("null")}",
      s"maxCollector=${maxCollector.map(_.toString).getOrElse("null")}",
      s"types=[${types.toList.map(_.schemaKey.toSchemaUri).mkString(",")}]"
    ).mkString(";")

  def withCheckpointer[D](d: D): State[D] =
    State(total, bad, minCollector, maxCollector, types, d)
}

object State {

  def fromEvents(rows: List[TransformationResult]): State[Unit] =
    Monoid[State[Unit]].combineAll(rows.map(fromEvent))

  def fromEvent(row: TransformationResult): State[Unit] =
    row match {
      case Left(_) =>
        Monoid[State[Unit]].empty.copy(total = 1, bad = 1)
      case Right(SuccessfulTransformation(event, _)) =>
        State(1, 0, Some(event.collector_tstamp), Some(event.collector_tstamp), event.inventory, ())
    }

  private def combineOption[A](
    f: (A, A) => A,
    a: Option[A],
    b: Option[A]
  ): Option[A] =
    (a, b).mapN(f).orElse(a).orElse(b)

  implicit def stateSemigroup[C: Monoid]: Monoid[State[C]] =
    new Monoid[State[C]] {
      def empty: State[C] = State(0L, 0L, None, None, Set.empty, Monoid[C].empty)

      def combine(x: State[C], y: State[C]): State[C] =
        State(
          x.total + y.total,
          x.bad + y.bad,
          combineOption(Ordering[Instant].min, x.minCollector, y.minCollector),
          combineOption(Ordering[Instant].max, x.maxCollector, y.maxCollector),
          x.types ++ y.types,
          x.checkpointer |+| y.checkpointer
        )
    }

  implicit val stateShow: Show[State[Any]] =
    Show.show(_.show)
}
