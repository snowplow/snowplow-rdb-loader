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

import cats.implicits._

import cats.effect.Concurrent
import cats.effect.concurrent.{Ref, Semaphore}

import fs2.concurrent.Queue

/**
 * A connection pool like entity, managing acquisition and use of several
 * resources `R`. The `use` function is completely transparent to user
 * code and identical to `Resource#use`, but in case of `Pool` a function
 * would do one of the following:
 * 1. Receive a free pre-allocated resource
 * 2. Trigger creation of a new one if capacity allows
 * 3. Block (semantically) until other fibers release a resource if capacity
 * doesn't allow
 *
 * @tparam F an effect type, usually `IO`
 * @tparam R a resource type, such as DB
 */
trait Pool[F[_], R] {
  def use[O](f: R => F[O]): F[O]
}

object Pool {

  def createQ[F[_]: Concurrent, R](acquire: F[R], release: R => F[Unit], max: Int): F[Pool[F, R]] = {
    val resourceP = acquire.map(res => ResourceP(res, release(res)))
    for {
      resourceQueue <- Queue.bounded[F, ResourceP[F, R]](max)
      semaphore     <- Semaphore(max.toLong)
      availableR    <- Ref.of[F, Int](max)
    } yield new Pool[F, R] {
      def use[O](f: R => F[O]): F[O] = {
        def useAndReturn(r: ResourceP[F, R]): F[O] =
          Concurrent[F].attempt(f(r.resource)).flatMap {
            case Right(result) => resourceQueue.enqueue1(r).as(result)
            case Left(error) => availableR.update(_ + 1) *> r.release *> Concurrent[F].raiseError(error)
          }

        // The semaphore protects otherwise thread-unsafe Ref.get -> Ref.update chain
        // Otherwise max+1 fibers could run into (available <= 0) branch deadlocking dequeue
        semaphore.acquire *>
          resourceQueue.tryDequeue1.flatMap {
            case Some(r) =>
              useAndReturn(r)
            case None =>
              availableR.get.flatMap { available =>
                if (available <= 0) resourceQueue.dequeue1.flatMap(useAndReturn)
                else availableR.update(_ - 1) *> resourceP.flatMap(useAndReturn) <* availableR.update(_ + 1)
              }
          } <* semaphore.release
      }
    }
  }

  private case class ResourceP[F[_], R](resource: R, release: F[Unit])
}
