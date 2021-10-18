/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.db

import scala.concurrent.duration._

import cats.implicits._

import cats.effect.{Timer, IO, Concurrent, Sync}
import cats.effect.concurrent.Ref
import cats.effect.implicits._

import com.snowplowanalytics.snowplow.rdbloader.db.Pool.create
import cats.effect.testing.specs2.CatsIO

import org.scalacheck.Arbitrary._
import org.scalacheck.{Gen, Shrink}
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck

class PoolSpec extends Specification with CatsIO with ScalaCheck {
  import PoolSpec.State.Message

  override val Timeout = 5.seconds

  "Pool" should {
    "create no more resources than max" in {
      val argsGen = for {
        resN <- Gen.chooseNum(2, 10)
        fibN <- Gen.chooseNum(resN, resN * 10)
      } yield (resN, fibN)

      prop { args: (Int, Int) =>
        val (resourceN, fibersN) = args
        PoolSpec
          .runConcurrentTest[IO](PoolSpec.runUse[IO](Some(20L)))(resourceN, fibersN)
          .map(_.getLog)
          .map { log =>
            val resources = log.collect { case Message.Using(id, _) => id }
            val iterations = log.collect { case Message.Using(_, iteration) => iteration }

            resources.toSet.size must beBetween(2, resourceN)
            iterations must haveSize(fibersN)
          }
      }.setGen(argsGen).setShrink(Shrink(_ => Stream.empty[(Int, Int)]))
    }

    "maintain a single resource with sequential jobs" in {
      val argsGen = for {
        resN <- Gen.chooseNum(1, 100)
        jobsN <- Gen.chooseNum(1, 15)
      } yield (resN, jobsN)

      prop { args: (Int, Int) =>
        val (resourceN, jobsN) = args
        PoolSpec
          .runSyncTest[IO](PoolSpec.runUse[IO](None))(resourceN, jobsN)
          .map(_.getLog)
          .map { log =>
            val resources = log.collect { case Message.Using(id, _) => id }
            val iterations = log.collect { case Message.Using(_, iteration) => iteration }

            resources.toSet.size must beEqualTo(1)
            iterations must haveSize(jobsN)
          }
      }.setGen(argsGen).setShrink(Shrink(_ => Stream.empty[(Int, Int)]))
    }

    "throws an exception in case of failure" in {
      val action = create[IO, Unit](IO.unit, _ => IO.unit, 1).use { pool =>
        pool.use { _ =>
          IO.raiseError[Int](new RuntimeException("Boom!"))
        }
      }

      action.attempt.map { result =>
        result must beLeft
      }
    }

    "acquire new resources in case of a failure" in {
      // Fail every 3rd use
      def runUse(stateRef: Ref[IO, PoolSpec.State])(resource: Int): IO[Unit] =
        stateRef.updateAndGet(_.incrementUse).flatMap { state =>

          val throwOrNothing = if (state.use % 3 == 0)
            stateRef.update(_.write(PoolSpec.State.Message.Failure(resource, state.use))) *>
              IO.raiseError(new RuntimeException(s"Interrupting resource $resource at ${state.use}"))
          else IO.unit

          throwOrNothing *> stateRef.update(_.write(PoolSpec.State.Message.Using(resource, state.use)))
        }

      val resourceN = 2
      val jobsN = 7
      val expected = List(
        Message.Using(1,1), Message.Using(1,2), Message.Failure(1,3), Message.Released(1),
        Message.Using(2,4), Message.Using(2,5), Message.Failure(2,6), Message.Released(2),
        Message.Using(3,7),
        Message.Released(3)   // Happens during pool destruction
      )

      PoolSpec
        .runSyncTest[IO](runUse, true)(resourceN, jobsN)
        .map { state =>
          state.getLog must beEqualTo(expected)

          state.use must beEqualTo(jobsN)
          state.resourceId must beEqualTo(resourceN + 1)
        }
    }
  }
}

object PoolSpec {

  /**
   * An internal test suite state, reflecting everything happening insie `Pool`
   * @param resourceId last created resource (our `R` is simple `Int`)
   *                   must be incremented during creation
   * @param use how many times *all* resources were used, i.e. how many times `use`
   *            has been called
   * @param log trail of all `f`s
   */
  final case class State(resourceId: Int, use: Int, log: List[State.Message]) {
    def increment: State =
      State(resourceId + 1, use, log)

    def incrementUse: State =
      State(resourceId, use + 1, log)

    def getLog: List[State.Message] =
      log.reverse

    def write(event: State.Message): State =
      State(resourceId, use, event :: log)
  }

  object State {
    def init: State =
      State(0, 0, Nil)

    /** A log message for a pool action */
    sealed trait Message {
      /** During the action, a resourceId has been used */
      def resourceId: Int
    }
    object Message {
      case class Using(resourceId: Int, iteration: Int) extends Message
      case class Released(resourceId: Int) extends Message
      case class Failure(resourceId: Int, iteration: Int) extends Message
    }
  }

  /** An action to create a resource (`R` is `Int`) and reflect it in `State` */
  def resource[F[_]: Sync](stateRef: Ref[F, State]): (F[Int], Int => F[Unit]) = {
    val acquire = stateRef.updateAndGet(_.increment).map(_.resourceId)
    val release = (id: Int) => stateRef.update(_.write(State.Message.Released(id)))
    (acquire, release)
  }

  /** Run `fibersN` concurrent actions */
  def runConcurrentTest[F[_]: Concurrent: Timer](runUse: Ref[F, State] => Int => F[Unit])
                                                (resourcesN: Int, fibersN: Int): F[State] =
    for {
      state <- Ref.of[F, State](State.init)
      (acquire, release) = resource[F](state)
      _ <- create[F, Int](acquire, release, resourcesN).use { pool =>
        pool
          .use(runUse(state))
          .start
          .replicateA(fibersN)
          .flatMap(fibers => fibers.traverse_(f => Concurrent[F].attempt(f.join)).void)
      }
      latest <- state.get
    } yield latest

  /**
   * Run `jobsN` sequential actions with at most `resourceN` resources
   * If `recover` is true an exception will be silently recovered
   */
  def runSyncTest[F[_]: Concurrent: Timer](runUse: Ref[F, State] => Int => F[Unit], recover: Boolean = false)
                                          (resourcesN: Int, jobsN: Int): F[State] =
    for {
      state <- Ref.of[F, State](State.init)
      (acquire, release) = resource[F](state)
      _ <- create[F, Int](acquire, release, resourcesN).use { pool =>
        val run = pool.use(runUse(state))
        val action = if (recover) Concurrent[F].attempt(run).void else run
        action.replicateA(jobsN)
      }
      latest <- state.get
    } yield latest

  /** A use function with potential blocking action */
  def runUse[F[_]: Timer: Sync](sleepMs: Option[Long])(stateRef: Ref[F, State])(resource: Int): F[Unit] =
    for {
      state <- stateRef.updateAndGet(_.incrementUse)
      _     <- sleepMs.map(_.millis).fold(Sync[F].unit)(Timer[F].sleep)
      _     <- stateRef.update(_.write(State.Message.Using(resource, state.use)))
    } yield ()
}
