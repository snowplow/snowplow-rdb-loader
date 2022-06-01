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
package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.generic

import fs2.Pipe
import cats.implicits._

import cats.effect.concurrent.Ref
import cats.effect.{IO, ContextShift, Timer}

import org.specs2.mutable.Specification

class KeyedEnqueueSpec extends Specification {
  import KeyedEnqueueSpec._

  "sink" should {

    "write elements to the pipe when they share the same key" in {
      val action = for {
        (store, getSink) <- KeyedEnqueueSpec.sinkAndStore
        keyedEnqueue <- KeyedEnqueue.empty[String, String].pure[IO]
        keyedEnqueue <- KeyedEnqueue.enqueueKV[IO, String, String](keyedEnqueue, "key-1", "element-1")
        keyedEnqueue <- KeyedEnqueue.enqueueKV[IO, String, String](keyedEnqueue, "key-1", "element-2")
        _            <- KeyedEnqueue.sink(keyedEnqueue, getSink).compile.drain
        finalResult <- store.get
      } yield finalResult

      val stored = action.unsafeRunSync()
      stored.sorted must beEqualTo(Vector(
        ("key-1", "element-1"),
        ("key-1", "element-2")
      ))
    }

    "write elements to the pipe when they have different keys" in {
      val action = for {
        (store, getSink) <- KeyedEnqueueSpec.sinkAndStore
        keyedEnqueue <- KeyedEnqueue.empty[String, String].pure[IO]
        keyedEnqueue <- KeyedEnqueue.enqueueKV[IO, String, String](keyedEnqueue, "key-1", "element-1")
        keyedEnqueue <- KeyedEnqueue.enqueueKV[IO, String, String](keyedEnqueue, "key-2", "element-2")
        _            <- KeyedEnqueue.sink(keyedEnqueue, getSink).compile.drain
        finalResult <- store.get
      } yield finalResult

      val stored = action.unsafeRunSync()
      stored.sorted must beEqualTo(Vector(
        ("key-1", "element-1"),
        ("key-2", "element-2")
      ))
    }

  }
}

object KeyedEnqueueSpec {
  implicit val CS: ContextShift[IO] = IO.contextShift(concurrent.ExecutionContext.global)
  implicit val T: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)

  def sinkAndStore: IO[(Ref[IO, Vector[(String, String)]], String => Pipe[IO, String, Unit])] = {
    Ref.of[IO, Vector[(String, String)]](Vector.empty).map { ref =>
      def pipe(key: String): Pipe[IO, String, Unit] =
        _.evalMap { s =>
          ref.update { items =>
            items :+ ((key, s))
          }
        }
      (ref, pipe)
    }
  }


}
