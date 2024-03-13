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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing

import cats.effect.{IO, Resource}
import cats.effect.kernel.Ref

import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.AppId
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.FileUtils
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.FileUtils.{createTempDirectory, directoryStream}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing.BaseProcessingSpec.{
  ProcessingOutput,
  TransformerConfig
}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sources.ParsedC
import fs2.Stream
import org.specs2.mutable.Specification

import scala.concurrent.duration.DurationInt

import java.util.Base64
import fs2.io.file.Path

trait BaseProcessingSpec extends Specification {

  def removeAppId(s: List[String]): List[String] = s.map(_.replace(s"-${AppId.appId}", ""))

  protected val temporaryDirectory: Resource[IO, Path] = createTempDirectory

  protected def process(input: Stream[IO, ParsedC[Unit]], config: TransformerConfig): IO[ProcessingOutput] = {
    val args = prepareAppArgs(config)

    for {
      checkpointRef <- Ref.of[IO, Int](0)
      completionsRef <- Ref.of[IO, Vector[String]](Vector.empty)
      queueBadSink <- Ref.of[IO, Vector[String]](Vector.empty)
      _ <- TestApplication.run(args, completionsRef, checkpointRef, queueBadSink, input).timeout(60.seconds)
      checkpointed <- checkpointRef.get
      completions <- completionsRef.get
      badrows <- queueBadSink.get
    } yield ProcessingOutput(completions, badrows, checkpointed)
  }

  protected def assertStringRows(actualRows: List[String], expectedRows: List[String]) =
    actualRows
      .zip(expectedRows)
      .map { case (actual, expected) =>
        actual must beEqualTo(expected)
      }
      .reduce(_ and _)

  protected def readMessageFromResource(resource: String, outputRootDirectory: Path) =
    readLinesFromResource(resource)
      .map(_.mkString)
      .map(
        _.replace("output_path_placeholder", outputRootDirectory.toNioPath.toUri.toString.replaceAll("/+$", ""))
          .replace("version_placeholder", BuildInfo.version)
          .replace(" ", "")
      )

  protected def readStringRowsFrom(path: Path): IO[List[String]] =
    directoryStream(path).compile.toList

  protected def readLinesFromResource(resource: String) =
    FileUtils.readLines(resource)

  protected def pathExists(path: Path): IO[Boolean] =
    FileUtils.pathExists(path)

  protected def prepareAppArgs(config: TransformerConfig) = {
    val encoder = Base64.getUrlEncoder

    List(
      "--iglu-config",
      new String(encoder.encode(config.iglu.getBytes)),
      "--config",
      new String(encoder.encode(config.app.replace("file:/", "s3:/").getBytes))
    )
  }
}

object BaseProcessingSpec {

  final case class TransformerConfig(app: String, iglu: String)
  final case class ProcessingOutput(
    completionMessages: Vector[String],
    badrowsFromQueue: Vector[String],
    checkpointed: Int
  )

}
