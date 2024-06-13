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
package com.snowplowanalytics.snowplow

import cats._
import cats.data._
import cats.implicits._

import fs2.Stream

import doobie.util.{Get, Put, Read}
import doobie.implicits.javasql._

import io.circe.parser.parse

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.snowplow.rdbloader.cloud.authservice.LoadAuthService
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{Count, ManifestType, Timestamps}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.{Semver, StringEnum}
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.{Statement, Target}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, DiscoveryFailure}

package object rdbloader {

  /** Stream of discovered folders. `LoaderMessage` is here for metainformation */
  type DiscoveryStream[F[_]] = Stream[F, DataDiscovery.WithOrigin]

  /**
   * List of DB-agnostic load statements. Could be just single `COPY events` or also shredded tables
   */
  type LoadStatements = NonEmptyList[LoadAuthService.LoadAuthMethod => Statement.Loading]

  /** A function to build a specific `Target` or error in case invalid config is passed */
  type BuildTarget[I] = Config[StorageTarget] => Either[String, Target[I]]

  /** Loading effect, producing value of type `A` with possible `LoaderError` */
  type LoaderAction[F[_], A] = EitherT[F, LoaderError, A]

  /** Lift value into */
  object LoaderAction {
    def apply[F[_], A](actionE: F[Either[LoaderError, A]]): LoaderAction[F, A] =
      EitherT[F, LoaderError, A](actionE)

    def pure[F[_]: Applicative, A](a: A): LoaderAction[F, A] =
      EitherT.pure[F, LoaderError](a)

    def liftF[F[_]: Applicative, A](fa: F[A]): LoaderAction[F, A] =
      EitherT.liftF[F, LoaderError, A](fa)
  }

  /** IO-free result validation */
  type DiscoveryStep[A] = Either[DiscoveryFailure, A]

  /** Single discovery step */
  type DiscoveryAction[F[_], A] = F[DiscoveryStep[A]]

  implicit val putFolder: Put[BlobStorage.Folder] =
    Put[String].tcontramap(_.toString)

  implicit val getFolder: Get[BlobStorage.Folder] =
    Get[String].temap(BlobStorage.Folder.parse)

  implicit val getCompression: Get[Compression] =
    Get[String].temap(str => StringEnum.fromString[Compression](str))

  implicit val getListManifestType: Get[List[ManifestType]] =
    Get[String].temap(str => parse(str).flatMap(_.as[List[ManifestType]]).leftMap(_.show))

  implicit val putKey: Put[BlobStorage.Key] =
    Put[String].tcontramap(_.toString)

  implicit val putCompression: Put[Compression] =
    Put[String].tcontramap(_.asString)

  implicit val getSchemaKey: Get[SchemaKey] =
    Get[String].temap(s => SchemaKey.fromUri(s).leftMap(e => s"Cannot parse $s into Iglu schema key, ${e.code}"))

  implicit val readSchemaKey: Read[SchemaKey] =
    Read.fromGet(getSchemaKey)

  // To replace Instant with sql.Timetstamp
  implicit val readTimestamps: Read[Timestamps] = {
    val tstampDecoder    = Read[java.sql.Timestamp]
    val tstampOptDecoder = Read.fromGetOption[java.sql.Timestamp]
    (tstampDecoder, tstampDecoder, tstampOptDecoder, tstampOptDecoder).mapN { case (a, b, c, d) =>
      Timestamps(a.toInstant, b.toInstant, c.map(_.toInstant), d.map(_.toInstant))
    }
  }

  implicit val putSemver: Put[Semver] =
    Put[String].tcontramap(_.show)

  implicit val getSemver: Get[Semver] =
    Get[String].temap(Semver.decodeSemver)

  // In order to preserve backward compatibility, bad events count is ignored
  implicit val getCount: Get[Count] =
    Get[Long].tmap(Count(_, None))

  implicit val putCount: Put[Count] =
    Put[Long].contramap(_.good)
}
