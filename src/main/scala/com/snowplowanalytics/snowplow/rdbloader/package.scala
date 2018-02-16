/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow

import cats.Functor
import cats.data._
import cats.free.Free
import cats.implicits._

import rdbloader.LoaderError.DiscoveryFailure

package object rdbloader {
  /**
   * Main RDB Loader type. Represents all IO happening
   * during discovering, loading and monitoring.
   * End of the world type, that must be unwrapped and executed
   * using one of interpreters
   */
  type Action[A] = Free[LoaderA, A]

  /**
    * Loading effect, producing value of type `A` with possible `LoaderError`
    *
    * @tparam A value of computation
    */
  type LoaderAction[A] = EitherT[Action, LoaderError, A]

  /** Lift value into  */
  object LoaderAction {
    def unit: LoaderAction[Unit] =
      EitherT.liftF(Free.pure(()))

    def lift[A](value: A): LoaderAction[A] =
      EitherT.liftF(Free.pure(value))

    def liftE[A](either: Either[LoaderError, A]): LoaderAction[A] =
      EitherT(Free.pure(either))

    def liftA[A](action: Action[A]): LoaderAction[A] =
      EitherT(action.map(_.asRight[LoaderError]))
  }

  /** Non-short-circuiting version of `TargetLoading` */
  type ActionE[A] = Free[LoaderA, Either[LoaderError, A]]

  object ActionE {
    def liftError(error: LoaderError): ActionE[Nothing] =
      Free.pure(error.asLeft)
  }


  /**
   * IO-free result validation
   */
  type DiscoveryStep[A] = Either[DiscoveryFailure, A]


  /** Single discovery step */
  type DiscoveryAction[A] = Action[DiscoveryStep[A]]

  /**
   * Composed functor of IO and discovery step
   */
  private[rdbloader] val DiscoveryAction =
    Functor[Action].compose[DiscoveryStep]


  implicit class AggregateErrors[A, B](eithers: List[Either[A, B]]) {
    def aggregatedErrors: ValidatedNel[A, List[B]] =
      eithers.map(_.toValidatedNel).sequence
  }
}
