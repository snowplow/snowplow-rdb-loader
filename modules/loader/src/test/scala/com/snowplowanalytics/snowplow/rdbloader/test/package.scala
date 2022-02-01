package com.snowplowanalytics.snowplow.rdbloader

import cats.data.{EitherT, State => CState}
import cats.effect.{Clock, Timer}
import com.snowplowanalytics.snowplow.rdbloader.algebras.db._
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Cache, Iglu, Logging, Monitoring}
import com.snowplowanalytics.snowplow.rdbloader.state.Control
import com.snowplowanalytics.snowplow.rdbloader.test.dao.{
  PureFolderMonitoringDao,
  PureMigrationBuilder,
  PureTargetLoader
}

package object test {

  /** Pure effect. It can only change [[TestState]] and never actually produce side-effects */
  type Pure[A] = EitherT[CState[TestState, *], Throwable, A]

  object Pure {
    def apply[A](f: TestState => (TestState, A)): Pure[A] = EitherT.right(CState(f))
    def liftWith[I, A](f: I   => A)(a: I): Pure[A]        = EitherT.right(CState((s: TestState) => (s, f(a))))
    def pure[A](a: A): Pure[A] = EitherT.pure(a)
    def modify(f: TestState => TestState): Pure[Unit] = EitherT.right(CState.modify(f))
    def log(msg: String): Pure[Unit]   = apply(s => (s.log(msg), ()))
    def sql(msg: String): Pure[Unit]   = apply(s => (s.sql(msg), ()))
    def fail[A](a: Throwable): Pure[A] = EitherT.leftT(a)
    def unit: Pure[Unit]               = pure(())
    def transact: Pure[Unit]           = apply(s => (s.transact, ()))
    def get[A](f: TestState => A): Pure[A] = apply(s => (s, f(s)))
  }

  type EitherThrow[A] = Either[Throwable, A]

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
//  implicit def awsPure: AWS[Pure]                       = PureAWS.interpreter(PureAWS(_ => Stream.empty, _ => false))
  implicit def cachePure: Cache[Pure]                   = PureCache.interpreter
  implicit def clockPure: Clock[Pure]                   = PureClock.interpreter
  implicit def igluPure: Iglu[Pure]                     = PureIglu.interpreter
  implicit def ctlPure: Control[Pure]                   = PureControl.interpreter
  implicit def logPure: Logging[Pure]                   = PureLogging.interpreter()
  implicit def mnPure: Monitoring[Pure]                 = PureMonitoring.interpreter
  implicit def timerPure: Timer[Pure]                   = PureTimer.interpreter
  implicit def txPure: Transaction[Pure, Pure]          = PureTransaction.interpreter
  implicit def folderDaoPure: FolderMonitoringDao[Pure] = PureFolderMonitoringDao.interpreter
  implicit def migrationPure: MigrationBuilder[Pure]    = PureMigrationBuilder.interpreter
  implicit def tgtLoaderPure: TargetLoader[Pure]        = PureTargetLoader.interpreter
}
