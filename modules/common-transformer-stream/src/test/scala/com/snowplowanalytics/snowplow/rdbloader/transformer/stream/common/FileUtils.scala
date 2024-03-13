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
