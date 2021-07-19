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

import cats.effect.{ExitCase, Timer, Concurrent, Resource}
import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.implicits._

import fs2.concurrent.{InspectableQueue}

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
 * The `Pool` does not implement any catch/retry logic, except for one necessary
 * for releasing. If the action hasn't been recovered (e.g. with `attempt`) it
 * will crash the whole program
 *
 * @tparam F an effect type, usually `IO`
 * @tparam R a resource type, such as DB
 */
trait Pool[F[_], R] {
  /**
   * Use a resource `R` provided by the [[Pool]]
   * Semantic is similar to `Resource#use`, but instead of releasing the
   * resource, pool will return it to an awaiting queue
   *
   * If `f` fails the resource will be released and exception rethrown
   * It means that if use code need to preserve the resource, it should
   * catch the resource via `use(r => work(r).attempt)`. If resource needs
   * to be reacquired the catch needs to be outside: `use(r => work(r)).attempt`
   *
   * @param f an action using pool-provided resource
   */
  def use[O](f: R => F[O]): F[O]

  /**
   * Handle the inner resource `R` as a resource, where acquisition
   * action is either dequeue or action acquisition and where
   * release action is an enqueue in case of successful action or
   * actual release in case of a failure
   */
  def resource: Resource[F, R]

  private[db] def destroy: F[Unit]
}

object Pool {

  /** A timeout with which every release will be released during `Pool` destruction */
  val ReleaseTimeout: FiniteDuration = 5.seconds

  /**
   * Create a [[Pool]] with at most `max` available resources
   *
   * @param acquire a resource acquisition function
   * @param release a resource release function, which will be used after every
   *                `use` failure and at pool destruction
   * @param max maximum number of resources allowed to hold
   *            if there's less than `max` resources necessary to perform `use`,
   *            i.e. the usage is sequential, not concurrent - the amount of resources
   *            will me smaller
   * @tparam R a type of resource, such as DB connection
   */
  def create[F[_]: Concurrent: Timer, R](acquire: F[R], release: R => F[Unit], max: Int): Resource[F, Pool[F, R]] = {
    val createPool = for {
      resourceQueue <- InspectableQueue.bounded[F, ResourceP[F, R]](max)
      semaphore     <- Semaphore(max.toLong)
      availableR    <- Ref.of[F, Int](max)
    } yield new Pool[F, R] {

      // increase/decrease amount of available, but uncreated resources
      val increase: F[Unit] = availableR.update(_ + 1)
      val decrease: F[Unit] = availableR.update(_ - 1)
      val allocateNew: F[ResourceP[F, R]] =
        (acquire <* decrease).map(res => ResourceP(res, release(res) *> increase))

      def resource: Resource[F, R] =
        // The semaphore protects otherwise thread-unsafe Ref.get -> Ref.update chain,
        // where max+1 fibers could run into (available <= 0) branch deadlocking dequeue
        Resource
          .makeCase(semaphore.acquire *> dequeue) {
            case (r, ExitCase.Completed) =>
              resourceQueue.enqueue1(r) *>
                semaphore.release
            case (r, _) =>
              r.release.attempt *>
                acquire.flatMap(r => resourceQueue.enqueue1(ResourceP(r, release(r)))) *>
                semaphore.release
          }
          .map(_.resource)

      def use[O](f: R => F[O]): F[O] =
        resource.use(f)

      def dequeue: F[ResourceP[F, R]] =
        resourceQueue.tryDequeue1.flatMap {
          case Some(r) =>
            Concurrent[F].pure(r)
          case None =>
            availableR.get.flatMap { available =>
              if (available <= 0) resourceQueue.dequeue1
              else allocateNew
            }
        }

      def destroy: F[Unit] = {
        // Acquire all available permits as soon as they appear, without blocking
        // To prevent other fibers taking them first
        val acquireAll: F[Unit] =
          semaphore.acquire.replicateA(max).start.attempt.void

        def releaseAll(remaining: Int): F[Unit] =
          resourceQueue
            .tryDequeue1
            .flatMap {
              case Some(resource) =>
                resource.release *> releaseAll(remaining - 1)
              case None if remaining <= 0 =>
                Concurrent[F].unit
              case None =>
                resourceQueue
                  .dequeue1
                  .flatMap(_.release)
                  .timeout(ReleaseTimeout) *>
                  releaseAll(remaining - 1)
            }


        acquireAll *> availableR.getAndSet(-max).flatMap { uncreated =>
          val created = max - uncreated
          releaseAll(created)
        }
      }
    }

    Resource.make(createPool)(_.destroy)
  }

  // We cannot use cats.effect.Resource because it's lifetime is scoped by
  // single Resource#use invocation and we wouldn't be able to return it
  // I was considering to ungroup it by Resource#allocated, but afraid that
  // Resource chaining could have a negative impact, thus there's no flatMap
  // on ResourceP
  private case class ResourceP[F[_], R](resource: R, release: F[Unit])
}
