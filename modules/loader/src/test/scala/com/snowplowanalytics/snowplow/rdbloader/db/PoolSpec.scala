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

import cats.Applicative
import cats.implicits._

import cats.effect.{Timer, IO, Concurrent, Sync}
import cats.effect.concurrent.Ref
import cats.effect.implicits._

import com.snowplowanalytics.snowplow.rdbloader.db.Pool.{createQ, ResourceP}
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
          .runConcurrentTest[IO](PoolSpec.runUseNoSleep[IO])(resourceN, fibersN)
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
          .runSyncTest[IO](PoolSpec.runUseNoSleep[IO])(resourceN, jobsN)
          .map(_.getLog)
          .map { log =>
            val resources = log.collect { case Message.Using(id, _) => id }
            val iterations = log.collect { case Message.Using(_, iteration) => iteration }

            resources.toSet.size must beEqualTo(1)
            iterations must haveSize(jobsN)
          }
      }.setGen(argsGen).setShrink(Shrink(_ => Stream.empty[(Int, Int)]))
    }

    "acquire new resources in case of a failure" in {
      val argsGen = for {
        resN <- Gen.chooseNum(2, 10)
        fibN <- Gen.chooseNum(resN, resN * 10)
      } yield (resN, fibN)

      def runUse(stateRef: Ref[IO, PoolSpec.State])(resource: Int): IO[Unit] =
        for {
          state <- stateRef.updateAndGet(_.incrementUse)
          _     <- if (state.use % 10 == 0)
            stateRef.update(_.write(PoolSpec.State.Message.Failure(resource, state.use))) *>
              IO.raiseError(new RuntimeException(s"Interrupting resource $resource at ${state.use}"))
          else IO.unit
          _     <- stateRef.update(_.write(PoolSpec.State.Message.Using(resource, state.use)))
        } yield ()

      prop { args: (Int, Int) =>
        val (resourceN, fibersN) = args
        PoolSpec
          .runSyncTest[IO](runUse)(resourceN, fibersN)
          .map { state =>
            val log = state.getLog
            val resources =  log.collect { case Message.Using(id, _) => id }
            val iterations = log.collect { case Message.Using(_, iteration) => iteration }
            val failures =   log.collect { case Message.Failure(_, iteration) => iteration }
            val releases =   log.collect { case Message.Released(id) => id }

            resources.toSet.size must beLessThanOrEqualTo(resourceN)
            iterations must haveSize(fibersN)
          }
      }.setGen(argsGen).setShrink(Shrink(_ => Stream.empty[(Int, Int)]))
    }

//    "foo" in {
//
//    }
  }
}

object PoolSpec {
  case class State(resourceId: Int, use: Int, log: List[State.Message]) {
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

    sealed trait Message
    object Message {
      case class Using(resourceId: Int, iteration: Int) extends Message
      case class Released(resourceId: Int) extends Message
      case class Failure(resourceId: Int, iteration: Int) extends Message
    }
  }

  def resource[F[_]: Sync](stateRef: Ref[F, State]): (F[Int], Int => F[Unit]) = {
    val acquire = stateRef.updateAndGet(_.increment).map(_.resourceId)
    val release = (id: Int) => stateRef.update(_.write(State.Message.Released(id)))
    (acquire, release)
  }

  def noSleep[F[_]: Applicative](s: State): F[Option[Long]] =
    Applicative[F].pure(None)

  def runConcurrentTest[F[_]: Concurrent: Timer](runUse: Ref[F, State] => Int => F[Unit])
                                                (resourcesN: Int, fibersN: Int): F[State] =
    for {
      state <- Ref.of[F, State](State.init)
      (acquire, release) = resource[F](state)
      _ <- createQ[F, Int](acquire, release, resourcesN).flatMap { pool =>
        pool
          .use(runUse(state))
          .start
          .replicateA(fibersN)
          .flatMap(fibers => fibers.traverse_(f => Concurrent[F].attempt(f.join)).void)
      }
      latest <- state.get
    } yield latest

  def runSyncTest[F[_]: Concurrent: Timer](runUse: Ref[F, State] => Int => F[Unit])
                                          (resourcesN: Int, jobsN: Int): F[State] =
    for {
      state <- Ref.of[F, State](State.init)
      (acquire, release) = resource[F](state)
      _ <- createQ[F, Int](acquire, release, resourcesN).flatMap { pool =>
        pool
          .use(runUse(state))
          .replicateA(jobsN)
      }
      latest <- state.get
    } yield latest

  def runUse[F[_]: Timer: Sync](getSleep: State => F[Option[Long]], stateRef: Ref[F, State])(resource: Int): F[Unit] =
    for {
      state <- stateRef.updateAndGet(_.incrementUse)
      _     <- getSleep(state).map(x => x.map(_.millis).fold(Sync[F].unit)(Timer[F].sleep))
      _     <- stateRef.update(_.write(State.Message.Using(resource, state.use)))
    } yield ()

  def runUseNoSleep[F[_]: Sync: Timer](stateRef: Ref[F, PoolSpec.State])(resource: Int): F[Unit] =
    for {
      state <- stateRef.updateAndGet(_.incrementUse)
      _     <- stateRef.update(_.write(PoolSpec.State.Message.Using(resource, state.use)))
    } yield ()

}