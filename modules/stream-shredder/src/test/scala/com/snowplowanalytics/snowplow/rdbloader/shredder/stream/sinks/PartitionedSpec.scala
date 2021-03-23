/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

import cats.implicits._

import cats.effect.concurrent.Ref
import cats.effect.{ContextShift, Blocker, Timer, Concurrent, IO, Sync}

import fs2.concurrent.SignallingRef
import fs2.{Stream, Pipe}

import com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks.PartitionedSpec.wkvStream
import com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks.generic.Partitioned

import org.specs2.mutable.Specification

class PartitionedSpec extends Specification {
  "write" should {
    "not stop" in {
      implicit val CS = IO.contextShift(ExecutionContext.global)

      implicit val T: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)

      val action = for {
        ref <- Ref.of[IO, Map[Int, Map[Int, List[Int]]]](Map.empty)
        _ <- SignallingRef[IO, Boolean](true)
        sinkFun = PartitionedSpec.getSink(ref) _
        _ <- wkvStream[IO].through(Partitioned.write[IO, Int, Int, Int](sinkFun)).compile.drain.timeout(500.millis).attempt
        result <- ref.get
      } yield result


      val result = action.unsafeRunSync()
      println(result)

      ok
    }
  }
}

object PartitionedSpec {

  def kvStream[F[_]: Sync] =
    Stream
      .iterate(0)(_ + 1)
      .map { v => (v / 5, v) }

  def getSink(ref: Ref[IO, Map[Int, Map[Int, List[Int]]]])(window: Int)(key: Int): Pipe[IO, Int, Unit] =
    _.evalMap { int =>
      ref.update { map =>
        map.get(window) match {
          case Some(keys) =>
            keys.get(key) match {
              case Some(list) => map.updated(window, keys.updated(key, int :: list))
              case None => map.updated(window, keys.updated(key, List(int)))
            }
          case None =>
            map.updated(window, Map(key -> List(int)))
        }
      }
    }


  def wkvStream[F[_]: Sync] =
    Stream
      .iterate(0)(_ + 1)
      .map { v => (v / 5, v) }
      .evalMap { case (w, v) => random[F](8, 10).map(k => (w, k, v)) }
      .evalTap { x => Sync[F].delay(println(s"Created $x")) }

  def random[F[_]: Sync](start: Int, end: Int): F[Int] =
    Sync[F].delay(start + scala.util.Random.nextInt((end - start) + 1))
}
