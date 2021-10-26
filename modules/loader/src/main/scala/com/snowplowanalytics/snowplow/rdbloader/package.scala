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
package com.snowplowanalytics.snowplow

import cats._
import cats.data._
import cats.implicits._

import fs2.Stream

import doobie.util.{Read, Get, Put}
import doobie.implicits.javasql._

import io.circe.parser.parse

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.snowplow.rdbloader.common.{S3, Message}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{Format, ShreddedType, Count, Timestamps}
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.{StringEnum, Semver}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DiscoveryFailure, DataDiscovery}

package object rdbloader {

  /** Stream of discovered folders. `LoaderMessage` is here for metainformation */
  type DiscoveryStream[F[_]] = Stream[F, Message[F, DataDiscovery.WithOrigin]]

  /** Loading effect, producing value of type `A` with possible `LoaderError` */
  type LoaderAction[F[_], A] = EitherT[F, LoaderError, A]

  /** Lift value into  */
  object LoaderAction {
    def unit[F[_]: Applicative]: LoaderAction[F, Unit] =
      EitherT.liftF(Applicative[F].unit)

    def liftE[F[_]: Applicative, A](either: Either[LoaderError, A]): LoaderAction[F, A] =
      EitherT.fromEither[F](either)

    def liftF[F[_]: Functor, A](action: F[A]): LoaderAction[F, A] =
      EitherT.liftF[F, LoaderError, A](action)

    def apply[F[_], A](actionE: ActionE[F, A]): LoaderAction[F, A] =
      EitherT[F, LoaderError, A](actionE)

    def raiseError[F[_]: Applicative, A](error: LoaderError): LoaderAction[F, A] =
      liftE(error.asLeft)

    def pure[F[_]: Applicative, A](a: A): LoaderAction[F, A] =
      EitherT.pure[F, LoaderError](a)
  }

  implicit class ActionOps[F[_], A](a: F[A]) {
    def liftA(implicit F: Applicative[F]): LoaderAction[F, A] =
      LoaderAction.liftF(a)
  }

  /** Non-short-circuiting version of `TargetLoading` */
  type ActionE[F[_], A] = F[Either[LoaderError, A]]

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

  implicit val putFolder: Put[S3.Folder] =
    Put[String].tcontramap(_.toString)

  implicit val getFolder: Get[S3.Folder] =
    Get[String].temap(S3.Folder.parse)

  implicit val getFormat: Get[Format] =
    Get[String].temap(Format.fromString)

  implicit val getListShreddedType: Get[List[ShreddedType]] =
    Get[String].temap(str => parse(str).flatMap(_.as[List[ShreddedType]]).leftMap(_.show))

  implicit val getCompression: Get[Compression] =
    Get[String].temap(str => StringEnum.fromString[Compression](str))

  implicit val putKey: Put[S3.Key] =
    Put[String].tcontramap(_.toString)

  implicit val putCompression: Put[Compression] =
    Put[String].tcontramap(_.asString)

  implicit val getSchemaKey: Get[SchemaKey] =
    Get[String].temap(s => SchemaKey.fromUri(s).leftMap(e => s"Cannot parse $s into Iglu schema key, ${e.code}"))

  implicit val readSchemaKey: Read[SchemaKey] =
    Read.fromGet(getSchemaKey)

  // To replace Instant with sql.Timetstamp
  implicit val readTimestamps: Read[Timestamps] = {
    val tstampDecoder = Read[java.sql.Timestamp]
    val tstampOptDecoder = Read.fromGetOption[java.sql.Timestamp]
    (tstampDecoder, tstampDecoder, tstampOptDecoder, tstampOptDecoder).mapN { case (a, b, c, d) =>
      Timestamps(a.toInstant, b.toInstant, c.map(_.toInstant), d.map(_.toInstant))
    }
  }

  implicit val putSemver: Put[Semver] =
    Put[String].tcontramap(_.show)

  implicit val getSemver: Get[Semver] =
    Get[String].temap(Semver.decodeSemver)

  implicit val getCount: Get[Count] =
    Get[Long].tmap(Count)

  implicit val putCount: Put[Count] =
    Put[Long].contramap(_.good)
}
