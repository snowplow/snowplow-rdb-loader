package com.snowplowanalytics.snowplow.rdbloader

import cats.data.{EitherT, State => CState, StateT}
import cats.syntax.either._

package object test {

  /** Pure effect. It can only change [[TestState]] and never actually produce side-effects */
  type Pure[A] = EitherT[CState[TestState, *], Throwable, A]

  object Pure {
    def apply[A](f: TestState => (TestState, A)): Pure[A] = EitherT.right(CState(f))
    def liftWith[I, A](f: I => A)(a: I): Pure[A] = EitherT.right(CState((s: TestState) => (s, f(a))))
    def pure[A](a: A): Pure[A] = EitherT.pure(a)
    def modify(f: TestState => TestState): Pure[Unit] = EitherT.right(CState.modify(f))
    def fail[A](a: Throwable): Pure[A] = EitherT.leftT(a)
    def unit: Pure[Unit] = pure(())
  }

  type EitherThrow[A] = Either[Throwable, A]

  type Pure2[A] = StateT[EitherThrow, TestState, A]

  object Pure2 {
    def apply[A](f: TestState => (TestState, A)): Pure2[A] = StateT.apply { s: TestState => f(s).asRight }
    def liftWith[I, A](f: I => A)(a: I): Pure2[A] = StateT.apply { s: TestState => (s, f(a)).asRight }
    def pure[A](a: A): Pure2[A] = StateT.pure[EitherThrow, TestState, A](a)
    def modify(f: TestState => TestState): Pure2[Unit] = StateT.modify[EitherThrow, TestState](f)
    def fail[A](a: Throwable): Pure2[A] = StateT.apply { _: TestState => a.asLeft[(TestState, A)] }
  }

  implicit class PureEitherOps[A](st: Pure[Either[LoaderError, A]]) {
    def toAction: LoaderAction[Pure, A] = LoaderAction(st)
  }

  implicit class PureActionOps[A](st: LoaderAction[Pure, A]) {
    def run =
      st.value.value.run(TestState.init).value
    def runS =
      st.value.value.runS(TestState.init).value
  }

  implicit class PureOps[A](st: Pure[A]) {
    def run =
      st.value.run(TestState.init).value
    def runA =
      st.value.runA(TestState.init).value
    def runS =
      st.value.runS(TestState.init).value
  }
}
