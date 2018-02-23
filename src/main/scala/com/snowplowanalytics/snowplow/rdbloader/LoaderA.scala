/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader

import java.nio.file.Path

import cats.free.Free
import cats.data.EitherT
import cats.implicits._

import com.snowplowanalytics.manifest.core.{ Item, Application }

// This library
import Security.Tunnel
import loaders.Common.SqlString
import db.Decoder

/**
 * RDB Loader algebra. Used to build Free data-structure,
 * interpreted to IO-actions
 */
sealed trait LoaderA[A]

object LoaderA {

  // Discovery ops
  case class ListS3(bucket: S3.Folder) extends LoaderA[Either[LoaderError, List[S3.Key]]]
  case class KeyExists(key: S3.Key) extends LoaderA[Boolean]
  case class DownloadData(path: S3.Folder, dest: Path) extends LoaderA[Either[LoaderError, List[Path]]]

  // Processing Manifest ops
  case class ManifestDiscover(application: Application, predicate: Item => Boolean) extends LoaderA[Either[LoaderError, List[Item]]]
  case class ManifestProcess(item: Item, load: LoaderAction[Unit]) extends LoaderA[Either[LoaderError, Unit]]

  // Loading ops
  case class ExecuteUpdate(sql: SqlString) extends LoaderA[Either[LoaderError, Long]]
  case class CopyViaStdin(files: List[Path], sql: SqlString) extends LoaderA[Either[LoaderError, Long]]

  // JDBC ops
  case class ExecuteQuery[A](query: SqlString, ev: Decoder[A]) extends LoaderA[Either[LoaderError, A]]

  // FS ops
  case object CreateTmpDir extends LoaderA[Either[LoaderError, Path]]
  case class DeleteDir(path: Path) extends LoaderA[Either[LoaderError, Unit]]

  // Auxiliary ops
  case class Sleep(timeout: Long) extends LoaderA[Unit]
  case class Track(exitLog: Log) extends LoaderA[Unit]
  case class Dump(key: S3.Key, exitLog: Log) extends LoaderA[Either[String, S3.Key]]
  case class Exit(exitLog: Log, dumpResult: Option[Either[String, S3.Key]]) extends LoaderA[Int]
  case class Print(message: String) extends LoaderA[Unit]

  // Cache ops
  case class Put(key: String, value: Option[S3.Key]) extends LoaderA[Unit]
  case class Get(key: String) extends LoaderA[Option[Option[S3.Key]]]

  // Tunnel ops
  case class EstablishTunnel(tunnelConfig: Tunnel) extends LoaderA[Either[LoaderError, Unit]]
  case class CloseTunnel() extends LoaderA[Either[LoaderError, Unit]]

  // Security ops
  case class GetEc2Property(name: String) extends LoaderA[Either[LoaderError, String]]


  def listS3(bucket: S3.Folder): Action[Either[LoaderError, List[S3.Key]]] =
    Free.liftF[LoaderA, Either[LoaderError, List[S3.Key]]](ListS3(bucket))

  /** Check if S3 key exist */
  def keyExists(key: S3.Key): Action[Boolean] =
    Free.liftF[LoaderA, Boolean](KeyExists(key))

  /** Download S3 key into local path */
  def downloadData(source: S3.Folder, dest: Path): Action[Either[LoaderError, List[Path]]] =
    Free.liftF[LoaderA, Either[LoaderError, List[Path]]](DownloadData(source, dest))

  /** Discover data from manifest */
  def manifestDiscover(application: Application, predicate: Item => Boolean): Action[Either[LoaderError, List[Item]]] =
    Free.liftF[LoaderA, Either[LoaderError, List[Item]]](ManifestDiscover(application, predicate))

  /** Execute single query (against target in interpreter) */
  def executeQuery(query: SqlString): Action[Either[LoaderError, Long]] =
    Free.liftF[LoaderA, Either[LoaderError, Long]](ExecuteQuery(query))

  /** Add Processing manifest records due loading */
  def manifestProcess(item: Item, load: LoaderAction[Unit]): LoaderAction[Unit] =
    EitherT(Free.liftF[LoaderA, Either[LoaderError, Unit]](ManifestProcess(item, load)))

  /** Execute single SQL statement (against target in interpreter) */
  def executeUpdate(sql: SqlString): Action[Either[LoaderError, Long]] =
    Free.liftF[LoaderA, Either[LoaderError, Long]](ExecuteUpdate(sql))

  /** Execute multiple (against target in interpreter) */
  def executeQueries(queries: List[SqlString]): Action[Either[LoaderError, Unit]] = {
    val shortCircuiting = queries.traverse(query => EitherT(executeUpdate(query)))
    shortCircuiting.void.value
  }


  /** Execute query and parse results into `A` */
  def executeQuery[A](query: SqlString)(implicit ev: Decoder[A]): LoaderAction[A] =
    EitherT(Free.liftF[LoaderA, Either[LoaderError, A]](ExecuteQuery[A](query, ev)))


  /** Execute SQL transaction (against target in interpreter) */
  def executeTransaction(queries: List[SqlString]): Action[Either[LoaderError, Unit]] = {
    val begin = SqlString.unsafeCoerce("BEGIN")
    val commit = SqlString.unsafeCoerce("COMMIT")
    val transaction = (begin :: queries) :+ commit
    executeQueries(transaction)
  }


  /** Perform PostgreSQL COPY table FROM STDIN (against target in interpreter) */
  def copyViaStdin(files: List[Path], query: SqlString): Action[Either[LoaderError, Long]] =
    Free.liftF[LoaderA, Either[LoaderError, Long]](CopyViaStdin(files, query))


  /** Create tmp directory */
  def createTmpDir: Action[Either[LoaderError, Path]] =
    Free.liftF[LoaderA, Either[LoaderError, Path]](CreateTmpDir)

  /** Delete directory */
  def deleteDir(path: Path): Action[Either[LoaderError, Unit]] =
    Free.liftF[LoaderA, Either[LoaderError, Unit]](DeleteDir(path))


  /** Block thread for some time */
  def sleep(timeout: Long): Action[Unit] =
    Free.liftF[LoaderA, Unit](Sleep(timeout))

  /** Track result via Snowplow tracker */
  def track(result: Log): Action[Unit] =
    Free.liftF[LoaderA, Unit](Track(result))

  /** Dump log to S3 */
  def dump(key: S3.Key, result: Log): Action[Either[String, S3.Key]] =
    Free.liftF[LoaderA, Either[String, S3.Key]](Dump(key, result))

  /** Close RDB Loader app with appropriate state */
  def exit(result: Log, dumpResult: Option[Either[String, S3.Key]]): Action[Int] =
    Free.liftF[LoaderA, Int](Exit(result, dumpResult))

  /** Print message to stdout */
  def print(message: String): Action[Unit] =
    Free.liftF[LoaderA, Unit](Print(message))


  /** Put value into cache (stored in interpreter) */
  def putCache(key: String, value: Option[S3.Key]): Action[Unit] =
    Free.liftF[LoaderA, Unit](Put(key, value))

  /** Get value from cache (stored in interpreter) */
  def getCache(key: String): Action[Option[Option[S3.Key]]] =
    Free.liftF[LoaderA, Option[Option[S3.Key]]](Get(key))


  /** Create SSH tunnel to bastion host */
  def establishTunnel(tunnelConfig: Tunnel): Action[Either[LoaderError, Unit]] =
    Free.liftF[LoaderA, Either[LoaderError, Unit]](EstablishTunnel(tunnelConfig))

  /** Close single available SSH tunnel */
  def closeTunnel(): Action[Either[LoaderError, Unit]] =
    Free.liftF[LoaderA, Either[LoaderError, Unit]](CloseTunnel())


  /** Retrieve decrypted property from EC2 Parameter Store */
  def getEc2Property(name: String): Action[Either[LoaderError, String]] =
    Free.liftF[LoaderA, Either[LoaderError, String]](GetEc2Property(name))
}

