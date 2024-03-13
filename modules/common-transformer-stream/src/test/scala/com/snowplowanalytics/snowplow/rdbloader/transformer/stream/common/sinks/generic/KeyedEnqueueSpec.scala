/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks.generic

import fs2.Pipe
import cats.implicits._
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global

import cats.effect.IO

import org.specs2.mutable.Specification

class KeyedEnqueueSpec extends Specification {

  "sink" should {

    "write elements to the pipe when they share the same key" in {
      val action = for {
        (store, getSink) <- KeyedEnqueueSpec.sinkAndStore
        keyedEnqueue <- KeyedEnqueue.empty[String, String].pure[IO]
        keyedEnqueue <- KeyedEnqueue.enqueueKV[IO, String, String](keyedEnqueue, "key-1", "element-1")
        keyedEnqueue <- KeyedEnqueue.enqueueKV[IO, String, String](keyedEnqueue, "key-1", "element-2")
        _ <- KeyedEnqueue.sink(keyedEnqueue, getSink).compile.drain
        finalResult <- store.get
      } yield finalResult

      val stored = action.unsafeRunSync()
      stored.sorted must beEqualTo(
        Vector(
          ("key-1", "element-1"),
          ("key-1", "element-2")
        )
      )
    }

    "write elements to the pipe when they have different keys" in {
      val action = for {
        (store, getSink) <- KeyedEnqueueSpec.sinkAndStore
        keyedEnqueue <- KeyedEnqueue.empty[String, String].pure[IO]
        keyedEnqueue <- KeyedEnqueue.enqueueKV[IO, String, String](keyedEnqueue, "key-1", "element-1")
        keyedEnqueue <- KeyedEnqueue.enqueueKV[IO, String, String](keyedEnqueue, "key-2", "element-2")
        _ <- KeyedEnqueue.sink(keyedEnqueue, getSink).compile.drain
        finalResult <- store.get
      } yield finalResult

      val stored = action.unsafeRunSync()
      stored.sorted must beEqualTo(
        Vector(
          ("key-1", "element-1"),
          ("key-2", "element-2")
        )
      )
    }

  }
}

object KeyedEnqueueSpec {

  def sinkAndStore: IO[(Ref[IO, Vector[(String, String)]], String => Pipe[IO, String, Unit])] =
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
