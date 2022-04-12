package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis

import java.time.Instant

import scala.annotation.tailrec

import cats.{Order, Show, Applicative}
import cats.kernel.Monoid
import cats.implicits._

import cats.effect.Sync
import cats.effect.concurrent.Ref

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.generic.Record
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sources.Parsed
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.Window
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.generic.{Status, Record}

case class State(total: Long,
                 bad: Long,
                 minCollector: Option[Instant],
                 maxCollector: Option[Instant],
                 types: Set[Data.ShreddedType],
                 sinks: Int) {
  def show: String =
    List(
      s"total=$total",
      s"bad=$bad",
      s"minCollector=${minCollector.map(_.toString).getOrElse("null")}",
      s"maxCollector=${maxCollector.map(_.toString).getOrElse("null")}",
      s"types=[${types.toList.map(_.schemaKey.toSchemaUri).mkString(",")}]",
      s"sinks=$sinks"
    ).mkString(";")
}

object State {

  /** Current state of a window */
  type WState = (Window, Status, State)

  /** Global mutable state */
  type Windows[F[_]] = Ref[F, List[WState]]

  def init[F[_]: Sync]: F[State.Windows[F]] =
    Ref.of[F, List[WState]](List.empty)

  /** Shredder-specific version of [[combineF]] */
  def combineEvent[F[_]]: (List[WState], Record[F, Window, Parsed]) => List[WState] =
    combineF[F, Window, State, Parsed](fromEvent)

  /** Update mutable global state with metadata from a `record` */
  def update[F[_]: Applicative](state: Windows[F])(record: Record[F, Window, Parsed]): F[Unit] =
    state.update(stack => combineEvent(stack, record))

  /**
   * Lens-like modify function
   * @param find a predicate to find triple that should be updated
   * @param mod a modify function that should be used to update found triple
   * @param get a get function to get result
   */
  def updateState[A](find: WState => Boolean, mod: WState => WState, get: WState => A)
                    (list: List[WState]): (List[WState], A) = {
    @tailrec
    def go(list: List[WState], accum: List[WState]): (List[WState], A) =
      list match {
        case Nil =>
          throw new IllegalStateException(s"Couldn't find $find in global window state")
        case h :: tail if find(h) =>
          val modified = mod(h)
          val updatedStack = (modified :: accum).reverse ::: tail
          (updatedStack, get(modified))
        case other :: tail =>
          go(tail, other :: accum)
      }

    go(list, Nil)
  }

  /**
   * Build a window state from a `Record` or basically merges in a `Record` into a global state
   * Merging in a record can have one of several possible outcomes:
   * + In a most common (`Record.Data`) case it adds some metadata into current window's `S`
   * + In case of `Record.EndWindow` it can add a new element into a `stack`
   *
   * The function tries to be as abstract as possible, `combineEvent` is a concrete implementation
   *
   * @param fromItem a function transforming record's data into `S`, representing single record
   * @param stack existing *global* window state
   * @param record an actual record's data or end of window special record
   * @tparam F record's effect (e.g. commit) type, not used in the function
   * @tparam W window type with ordering requirement to check if current record is ahead
   *           of latest one in the `stack`
   * @tparam S current window's state, i.e. number of total records
   * @tparam I current record's data type
   */
  def combineF[F[_], W: Order, S: Monoid, I](fromItem: I => S)
                                            (stack: List[(W, Status, S)],
                                             record: Record[F, W, I]): List[(W, Status, S)] = {
    stack match {
      case (window, Status.Active, state) :: tail if window === record.window =>
        record match {
          case Record.Data(_, _, item) =>
            (window, Status.Active, Monoid[S].combine(state, fromItem(item))) :: tail
          case Record.EndWindow(_, next, _) =>
            List(
              (next, Status.Active, Monoid[S].empty),
              (window, Status.Sealed, state)
            ) ::: tail
        }

      case (h @ (_, Status.Closed, _)) :: tail =>
        record match {
          case Record.Data(window, _, item) =>
            (window, Status.Active, fromItem(item)) :: h :: tail
          case Record.EndWindow(window, next, _) =>
            List(
              (next, Status.Active, Monoid[S].empty),
              (window, Status.Sealed, Monoid[S].empty),
              h
            ) ::: tail
        }

      case Nil =>
        record match {
          case Record.Data(window, _, item) =>
            (window, Status.Active, fromItem(item)) :: Nil
          case Record.EndWindow(window, next, _) =>
            List(
              (next, Status.Active, Monoid[S].empty),
              (window, Status.Sealed, Monoid[S].empty)
            )
        }
    }
  }

  def fromEvent(row: Parsed): State =
    row match {
      case Left(_) =>
        Monoid[State].empty.copy(total = 1, bad = 1)
      case Right(event) =>
        State(1, 0, Some(event.collector_tstamp), Some(event.collector_tstamp), event.inventory, 0)
    }

  def combineOption[A](f: (A, A) => A, a: Option[A], b: Option[A]): Option[A] =
    (a, b).mapN(f).orElse(a).orElse(b)

  implicit val stateMonoid: Monoid[State] =
    new Monoid[State] {
      def empty: State = State(0L, 0L, None, None, Set.empty, 0)

      def combine(x: State, y: State): State =
        State(
          x.total + y.total,
          x.bad + y.bad,
          combineOption(Ordering[Instant].min, x.minCollector, y.minCollector),
          combineOption(Ordering[Instant].max, x.maxCollector, y.maxCollector),
          x.types ++ y.types,
          x.sinks + y.sinks
        )
    }

  implicit val wStateShow: Show[WState] =
    Show.show { case (window, status, state) => s"${window.getDir} $status, ${state.show}" }

  implicit val stateShow: Show[State] =
    Show.show(_.show)
}

