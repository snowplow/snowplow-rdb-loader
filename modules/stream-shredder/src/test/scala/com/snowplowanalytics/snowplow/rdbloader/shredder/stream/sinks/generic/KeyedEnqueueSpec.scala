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
package com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks.generic

import scala.concurrent.duration._

import fs2.Pipe
import cats.implicits._

import cats.effect.concurrent.Ref
import cats.effect.{IO, ContextShift, Timer}

import org.specs2.mutable.Specification

class KeyedEnqueueSpec extends Specification {
  import KeyedEnqueueSpec._

  "enqueueKV" should {
    "have no effect if sink is not called" in {
      val action = for {
        (store, getSink) <- KeyedEnqueueSpec.sinkAndStore
        keyedEnqueue <- KeyedEnqueue.mk[IO, String, String](getSink)
        _ <- keyedEnqueue.enqueueKV("key-1", "element-1")
        finalResult <- store.get
      } yield finalResult

      val result = action.unsafeRunSync()
      result must beEmpty
    }

    "throw an exception if tried to write to a terminated enqueue" in {
      val action = for {
        (store, getSink) <- KeyedEnqueueSpec.sinkAndStore
        keyedEnqueue <- KeyedEnqueue.mk[IO, String, String](getSink)
        _ <- keyedEnqueue.enqueueKV("key-1", "element-1")
        _ <- keyedEnqueue.terminate
        _ <- keyedEnqueue.enqueueKV("key-1", "element-3")
        _ <- keyedEnqueue.sink.compile.drain
        finalResult <- store.get
      } yield finalResult

      action.unsafeRunSync() must throwAn[IllegalStateException]
    }
  }

  "sink" should {

    "block the execution (without termination), but write elements" in {
      val action = for {
        (store, getSink) <- KeyedEnqueueSpec.sinkAndStore
        keyedEnqueue <- KeyedEnqueue.mk[IO, String, String](getSink)
        _ <- keyedEnqueue.enqueueKV("key-1", "element-1")
        _ <- keyedEnqueue.enqueueKV("key-1", "element-2")
        timeout <- keyedEnqueue.sink.compile.drain.timeout(500.millis).attempt
        finalResult <- store.get
      } yield (timeout.isLeft, finalResult)

      val (timeout, map) = action.unsafeRunSync()
      timeout must beTrue
      map must beEqualTo(Map("key-1" -> List("element-2", "element-1")))
    }

    "not block the execution when terminated" in {
      val action = for {
        (store, getSink) <- KeyedEnqueueSpec.sinkAndStore
        keyedEnqueue <- KeyedEnqueue.mk[IO, String, String](getSink)
        _ <- keyedEnqueue.enqueueKV("key-1", "element-1")
        _ <- keyedEnqueue.enqueueKV("key-1", "element-2")
        _ <- keyedEnqueue.terminate
        _ <- keyedEnqueue.sink.compile.drain
        finalResult <- store.get
      } yield finalResult

      val map = action.unsafeRunSync()
      map must beEqualTo(Map("key-1" -> List("element-2", "element-1")))
    }
  }
}

object KeyedEnqueueSpec {
  implicit val CS: ContextShift[IO] = IO.contextShift(concurrent.ExecutionContext.global)
  implicit val T: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)

  def sinkAndStore: IO[(Ref[IO, Map[String, List[String]]], String => Pipe[IO, String, Unit])] = {
    Ref.of[IO, Map[String, List[String]]](Map.empty).map { ref =>
      def pipe(key: String): Pipe[IO, String, Unit] =
        _.evalMap { s =>
          ref.update { map =>
            map.get(key) match {
              case Some(list) => map.updated(key, s :: list)
              case None => map.updated(key, List(s))
            }
          }
        }

      (ref, pipe)
    }
  }


}
