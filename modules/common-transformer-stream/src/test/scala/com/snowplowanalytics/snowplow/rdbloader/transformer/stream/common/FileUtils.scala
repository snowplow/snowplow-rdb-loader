/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import cats.effect.{Blocker, ContextShift, IO, Resource}
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import fs2.{Stream, text}

import java.nio.file.Path

object FileUtils {

  def createTempDirectory(blocker: Blocker)(implicit cs: ContextShift[IO]): Resource[IO, Path] =
    fs2.io.file.tempDirectoryResource[IO](blocker, Path.of(System.getProperty("java.io.tmpdir")))

  def directoryStream(blocker: Blocker, dir: Path)(implicit cs: ContextShift[IO]): Stream[IO, String] =
    fs2.io.file.directoryStream[IO](blocker, dir).flatMap(fileStream(blocker, _))

  def readLines(blocker: Blocker, resourcePath: String)(implicit cs: ContextShift[IO]): IO[List[String]] =
    resourceFileStream(blocker, resourcePath)
      .map(_.replace("version_placeholder", BuildInfo.version))
      .compile
      .toList

  def resourceFileStream(blocker: Blocker, resourcePath: String)(implicit cs: ContextShift[IO]): Stream[IO, String] =
    fileStream(blocker, Path.of(getClass.getResource(resourcePath).getPath))

  def fileStream(blocker: Blocker, filePath: Path)(implicit cs: ContextShift[IO]): Stream[IO, String] =
    fs2.io.file
      .readAll[IO](filePath, blocker, 4096)
      .through(text.utf8Decode)
      .through(text.lines)

}
