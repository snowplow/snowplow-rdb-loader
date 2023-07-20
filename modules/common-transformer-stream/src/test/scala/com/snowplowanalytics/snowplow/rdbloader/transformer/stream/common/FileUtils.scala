/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
