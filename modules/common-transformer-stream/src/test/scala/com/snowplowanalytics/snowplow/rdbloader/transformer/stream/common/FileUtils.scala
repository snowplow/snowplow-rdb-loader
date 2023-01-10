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

import cats.effect.{IO, Resource}
import fs2.Stream
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import fs2.io.file.{Files, Path}

object FileUtils {

  def createTempDirectory: Resource[IO, Path] =
    Files[IO].tempDirectory

  def directoryStream(dir: Path): Stream[IO, String] =
    Files[IO].list(dir).flatMap(fileStream)

  def readLines(resourcePath: String): IO[List[String]] =
    resourceFileStream(resourcePath)
      .map(_.replace("version_placeholder", BuildInfo.version))
      .compile
      .toList

  def resourceFileStream(resourcePath: String): Stream[IO, String] =
    fileStream(Path(getClass.getResource(resourcePath).getPath))

  def fileStream(filePath: Path): Stream[IO, String] =
    Files[IO].readUtf8Lines(filePath)

  def pathExists(filePath: Path): IO[Boolean] =
    Files[IO].exists(filePath)

}
