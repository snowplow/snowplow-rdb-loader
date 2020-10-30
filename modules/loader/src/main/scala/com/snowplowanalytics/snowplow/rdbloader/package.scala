/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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

import cats._
import cats.data._
import cats.implicits._

import rdbloader.discovery.DiscoveryFailure

package object rdbloader {

  /** Loading effect, producing value of type `A` with possible `LoaderError` */
  type LoaderAction[F[_], A] = EitherT[F, LoaderError, A]

  /** Lift value into  */
  object LoaderAction {
    def unit[F[_]: Applicative]: LoaderAction[F, Unit] =
      EitherT.liftF(Applicative[F].unit)

    def rightT[F[_]: Applicative, A](value: A): LoaderAction[F, A] =
      EitherT.rightT[F, LoaderError](value)

    def liftE[F[_]: Applicative, A](either: Either[LoaderError, A]): LoaderAction[F, A] =
      EitherT.fromEither[F](either)

    def liftF[F[_]: Applicative, A](action: F[A]): LoaderAction[F, A] =
      EitherT.liftF[F, LoaderError, A](action)

    def apply[F[_], A](actionE: ActionE[F, A]): LoaderAction[F, A] =
      EitherT[F, LoaderError, A](actionE)
  }

  implicit class ActionOps[F[_], A](a: F[A]) {
    def liftA(implicit F: Applicative[F]): LoaderAction[F, A] =
      LoaderAction.liftF(a)
  }

  /** Non-short-circuiting version of `TargetLoading` */
  type ActionE[F[_], A] = F[Either[LoaderError, A]]

  object ActionE {
    def liftError[F[_]: Applicative](error: LoaderError): ActionE[F, Nothing] =
      Applicative[F].pure(error.asLeft)
  }

  /**
    * Helper function to traverse multiple validated results inside a single `Action`
    *
    * @param f collection of results, e.g. `IO[List[Validated[Result]]]`
    * @param ff helper function to transform end result, e.g. `ValidatedNel[String, A] => Either[String, A]`
    * @tparam F outer action, such as `IO`
    * @tparam G collection, such as `List`
    * @tparam H inner-effect type, such as `Validation`
    * @tparam J result effect, without constraints
    * @tparam A result
    * @return traversed and transformed action, where `H` replaced with `J` by `ff`
    */
  def sequenceInF[F[_]: Functor,
                  G[_]: Traverse,
                  H[_]: Applicative,
                  J[_], A](f: F[G[H[A]]], ff: H[G[A]] => J[G[A]]): F[J[G[A]]] =
    f.map(x => ff(x.sequence))

  /** IO-free result validation */
  type DiscoveryStep[A] = Either[DiscoveryFailure, A]

  /** Single discovery step */
  type DiscoveryAction[F[_], A] = F[DiscoveryStep[A]]

  implicit class AggregateErrors[A, B](eithers: List[Either[A, B]]) {
    def aggregatedErrors: ValidatedNel[A, List[B]] =
      eithers.map(_.toValidatedNel).sequence
  }
}
