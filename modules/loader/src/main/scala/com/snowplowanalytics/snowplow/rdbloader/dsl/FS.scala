/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}

import cats.syntax.functor._
import cats.syntax.applicativeError._
import cats.effect.Sync

import com.snowplowanalytics.snowplow.rdbloader.{ LoaderAction, LoaderError }

trait FS[F[_]] {
  /** Create tmp directory */
  def createTmpDir: LoaderAction[F, Path]

  /** Delete directory */
  def deleteDir(path: Path): LoaderAction[F, Unit]
}

object FS {

  def apply[F[_]](implicit ev: FS[F]): FS[F] = ev

  def fileSystemInterpreter[F[_]: Sync]: FS[F] = new FS[F] {
    def createTmpDir: LoaderAction[F, Path] =
      Sync[F]
        .delay(Files.createTempDirectory("rdb-loader"))
        .attemptT
        .leftMap(e => LoaderError.LoaderLocalError("Cannot create temporary directory.\n" + e.toString): LoaderError)

    def deleteDir(path: Path): LoaderAction[F, Unit] =
      Sync[F]
        .delay(Files.walkFileTree(path, DeleteVisitor))
        .attemptT
        .leftMap(e => LoaderError.LoaderLocalError(s"Cannot delete directory [${path.toString}].\n" + e.toString): LoaderError)
        .void
  }

  private object DeleteVisitor extends SimpleFileVisitor[Path] {
    override def visitFile(file: Path, attrs: BasicFileAttributes) = {
      Files.delete(file)
      FileVisitResult.CONTINUE
    }
  }

}

