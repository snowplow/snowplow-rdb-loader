package com.snowplowanalytics.snowplow.rdbloader.shredder.stream

import java.time.Instant

import scala.annotation.tailrec

import cats.{Order, Applicative}
import cats.kernel.Monoid
import cats.implicits._

import cats.effect.Sync
import cats.effect.concurrent.Ref

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks.Window
import com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks.generic.{Status, Record}
import com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sources.Parsed

case class State(total: Long,
                 bad: Long,
                 minCollector: Option[Instant],
                 maxCollector: Option[Instant],
                 types: Set[SchemaKey],
                 sinks: Int)

object State {

  /** Current state of a window */
  type WState = (Window, Status, State)

  type Windows[F[_]] = Ref[F, List[WState]]

  def init[F[_]: Sync]: F[State.Windows[F]] =
    Ref.of[F, List[WState]](List.empty)

  def add[F[_]: Applicative](state: Windows[F])(record: Record[F, Window, Parsed]): F[Unit] =
    state.update(stack => pullState(stack, fromEvent, record))

  /**
   * Lens-like modify function
   * @param find a predicate to find triple that should be upadted
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

  def pullState[F[_], W: Order, S: Monoid, I](stack: List[(W, Status, S)],
                                              aggregate: (I, S) => S,
                                              record: Record[F, W, I]): List[(W, Status, S)] = {
    stack match {
      case (window, Status.Active, state) :: tail if window === record.window =>
        record match {
          case Record.Data(_, _, item) =>
            (window, Status.Active, aggregate(item, state)) :: tail
          case Record.EndWindow(_, next, _) =>
            List(
              (next, Status.Active, Monoid[S].empty),
              (window, Status.Sealed, state)
            ) ::: tail
        }

      case (h @ (_, Status.Closed, _)) :: tail =>
        record match {
          case Record.Data(window, _, item) =>
            (window, Status.Active, aggregate(item, Monoid[S].empty)) :: h :: tail
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
            (window, Status.Active, aggregate(item, Monoid[S].empty)) :: Nil
          case Record.EndWindow(window, next, _) =>
            List(
              (next, Status.Active, Monoid[S].empty),
              (window, Status.Sealed, Monoid[S].empty)
            )
        }
    }
  }

  val Zero: State = State(0L, 0L, None, None, Set.empty, 0)

  def fromEvent(row: Parsed, state: State): State =
    row match {
      case Left(_) =>
        state.copy(total = state.total + 1, bad = state.bad + 1)
      case Right(event) =>
        State(
          state.total + 1,
          state.bad,
          combine(Ordering[Instant].min, Some(event.collector_tstamp), state.minCollector),
          combine(Ordering[Instant].max, Some(event.collector_tstamp), state.maxCollector),
          state.types ++ event.inventory.map(_.schemaKey),
          state.sinks
        )
    }

  def append(x: State, y: State): State =
    State(
      x.total + x.total,
      x.bad + x.bad,
      combine(Ordering[Instant].min, x.minCollector, y.minCollector),
      combine(Ordering[Instant].max, x.maxCollector, y.maxCollector),
      x.types ++ y.types,
      x.sinks + y.sinks
    )

  def combine[A](f: (A, A) => A, a: Option[A], b: Option[A]): Option[A] =
    (a, b).mapN(f).orElse(a).orElse(b)

  implicit val stateMonoid: Monoid[State] =
    new Monoid[State] {
      def empty: State = Zero

      def combine(x: State, y: State): State =
        append(x, y)
    }
}

