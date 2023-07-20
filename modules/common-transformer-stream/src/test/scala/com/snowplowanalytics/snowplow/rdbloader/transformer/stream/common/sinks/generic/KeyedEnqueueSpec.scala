/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
