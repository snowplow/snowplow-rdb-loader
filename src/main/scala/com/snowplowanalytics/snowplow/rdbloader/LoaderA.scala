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
package com.snowplowanalytics.snowplow.rdbloader

import java.nio.file.Path

import cats.free.Free
import cats.implicits._

// This library
import LoaderError.DiscoveryError
import loaders.Common.SqlString

/**
 * RDB Loader algebra. Used to build Free data-structure,
 * interpreted to IO-actions
 */
sealed trait LoaderA[A]

object LoaderA {

  // Discovery ops
  case class ListS3(bucket: S3.Folder) extends LoaderA[Either[DiscoveryError, List[S3.Key]]]
  case class KeyExists(key: S3.Key) extends LoaderA[Boolean]
  case class DownloadData(path: S3.Folder, dest: Path) extends LoaderA[Either[LoaderError, List[Path]]]

  // Loading ops
  case class ExecuteQuery(query: SqlString) extends LoaderA[Either[LoaderError, Long]]
  case class CopyViaStdin(files: List[Path], query: SqlString) extends LoaderA[Either[LoaderError, Long]]

  // FS ops
  case object CreateTmpDir extends LoaderA[Either[LoaderError, Path]]
  case class DeleteDir(path: Path) extends LoaderA[Either[LoaderError, Unit]]

  // Auxiliary ops
  case class Sleep(timeout: Long) extends LoaderA[Unit]
  case class Track(exitLog: Log) extends LoaderA[Unit]
  case class Dump(key: S3.Key, exitLog: Log) extends LoaderA[Either[String, S3.Key]]
  case class Exit(exitLog: Log, dumpResult: Option[Either[String, S3.Key]]) extends LoaderA[Int]

  // Cache ops
  case class Put(key: String, value: Option[S3.Key]) extends LoaderA[Unit]
  case class Get(key: String) extends LoaderA[Option[Option[S3.Key]]]


  def listS3(bucket: S3.Folder): Action[Either[DiscoveryError, List[S3.Key]]] =
    Free.liftF[LoaderA, Either[DiscoveryError, List[S3.Key]]](ListS3(bucket))

  def keyExists(key: S3.Key): Action[Boolean] =
    Free.liftF[LoaderA, Boolean](KeyExists(key))

  def downloadData(source: S3.Folder, dest: Path): Action[Either[LoaderError, List[Path]]] =
    Free.liftF[LoaderA, Either[LoaderError, List[Path]]](DownloadData(source, dest))


  def executeQuery(query: SqlString): Action[Either[LoaderError, Long]] =
    Free.liftF[LoaderA, Either[LoaderError, Long]](ExecuteQuery(query))

  def executeQueries(queries: List[SqlString]): Action[Either[LoaderError, Unit]] =
    queries.traverse(executeQuery).map(eithers => eithers.sequence.map(_.combineAll))

  def executeTransaction(queries: List[SqlString]): Action[Either[LoaderError, Unit]] = {
    val begin = SqlString.unsafeCoerce("BEGIN")
    val commit = SqlString.unsafeCoerce("COMMIT")
    val transaction = (begin :: queries) :+ commit
    executeQueries(transaction)
  }


  def copyViaStdin(files: List[Path], query: SqlString): Action[Either[LoaderError, Long]] =
    Free.liftF[LoaderA, Either[LoaderError, Long]](CopyViaStdin(files, query))


  def createTmpDir: Action[Either[LoaderError, Path]] =
    Free.liftF[LoaderA, Either[LoaderError, Path]](CreateTmpDir)

  def deleteDir(path: Path): Action[Either[LoaderError, Unit]] =
    Free.liftF[LoaderA, Either[LoaderError, Unit]](DeleteDir(path))


  def sleep(timeout: Long): Action[Unit] =
    Free.liftF[LoaderA, Unit](Sleep(timeout))

  def track(result: Log): Action[Unit] =
    Free.liftF[LoaderA, Unit](Track(result))

  def dump(key: S3.Key, result: Log): Action[Either[String, S3.Key]] =
    Free.liftF[LoaderA, Either[String, S3.Key]](Dump(key, result))

  def exit(result: Log, dumpResult: Option[Either[String, S3.Key]]): Action[Int] =
    Free.liftF[LoaderA, Int](Exit(result, dumpResult))


  def putCache(key: String, value: Option[S3.Key]): Action[Unit] =
    Free.liftF[LoaderA, Unit](Put(key, value))

  def getCache(key: String): Action[Option[Option[S3.Key]]] =
    Free.liftF[LoaderA, Option[Option[S3.Key]]](Get(key))
}

