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
package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.processing

import cats.effect.concurrent.Deferred
import cats.effect.{Blocker, Clock, ContextShift, IO, Timer}
import cats.implicits.toTraverseOps
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.FileUtils
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.FileUtils.{createTempDirectory, directoryStream}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.Processing.Windowed
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.processing.BaseProcessingSpec.{ConfigProvider, CreatedDirectories, OutputRows, ProcessingOutput, TestResources, TransformerConfig}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sources.Parsed
import fs2.Stream
import org.specs2.mutable.Specification

import java.nio.file.Path
import java.util.Base64
import java.util.concurrent.TimeUnit

trait BaseProcessingSpec extends Specification {

  implicit val CS: ContextShift[IO] = IO.contextShift(concurrent.ExecutionContext.global)
  implicit val T: Timer[IO]         = IO.timer(concurrent.ExecutionContext.global)

  //returns always 1970-01-01-10:31
  implicit val clock: Clock[IO] = new Clock[IO] {
    override def realTime(unit: TimeUnit): IO[Long] = IO(unit.convert(37860L, TimeUnit.SECONDS)) 
    override def monotonic(unit: TimeUnit): IO[Long] = IO(unit.convert(37860L, TimeUnit.SECONDS))
  }
  
  val blocker = Blocker.liftExecutionContext(concurrent.ExecutionContext.global)
  val testResources = createTempDirectory(blocker).map(TestResources)

  protected def process(input: Stream[IO, Windowed[IO, Parsed]],
                        configProvider: ConfigProvider,
                        expectedOutputPaths: List[String]): IO[ProcessingOutput] =
    testResources.use { resources =>
      val config = configProvider(resources)
      val args = prepareAppArgs(config)

      for {
        waitingForCompletionMessage <- Deferred[IO, String]
        runningApp                  <- TestApplication.run(args, waitingForCompletionMessage, input).start
        completionMessage           <- waitingForCompletionMessage.get
        _                           <- runningApp.cancel
        createdDirectories          <- readDirectoriesFrom(resources.outputRootDirectory, expectedOutputPaths)
      } yield ProcessingOutput(resources.outputRootDirectory, completionMessage, createdDirectories)
    }

  protected def assertOutputLines(directoryWithActualData: String,
                                  expectedResource: String,
                                  createdDirectories: CreatedDirectories) = {
    val expectedLines = FileUtils.readLines(blocker, expectedResource).unsafeRunSync
    createdDirectories(directoryWithActualData) must beEqualTo(expectedLines)
  }

  protected def assertCompletionMessage(result: ProcessingOutput,
                                        expectedResource: String) = {
    val expectedMessage = FileUtils
      .readLines(blocker, expectedResource)
      .map(_.mkString)
      .map(
        _
          .replace("output_path_placeholder", result.outputRootDirectory.toUri.toString)
          .replace("version_placeholder", BuildInfo.version)
          .replace(" ", "")
      )
      .unsafeRunSync

    result.completionMessage must beEqualTo(expectedMessage)
  }

  private def prepareAppArgs(config: TransformerConfig) = {
    val encoder = Base64.getUrlEncoder

    List(
      "--iglu-config", new String(encoder.encode(config.iglu.getBytes)),
      "--config", new String(encoder.encode(config.app.getBytes))
    )
  }

  private def readDirectoriesFrom(rootDirectory: Path, 
                                  children: List[String]): IO[CreatedDirectories] = {
    children.traverse { child =>
      readRowsFrom(rootDirectory, child)
        .map(rows => (child, rows))
    }
      .map(_.toMap)
  }

  private def readRowsFrom(outputDirectory: Path, childDirectory: String): IO[OutputRows] = {
    val fullPath = Path.of(outputDirectory.toString, childDirectory)
    directoryStream(blocker, fullPath).compile.toList
  }

}

object BaseProcessingSpec {

  type ConfigProvider = TestResources => TransformerConfig
  type CreatedDirectories = Map[String, OutputRows]
  type OutputRows = List[String]

  final case class TestResources(outputRootDirectory: Path)
  final case class TransformerConfig(app: String, iglu: String)

  final case class ProcessingOutput(outputRootDirectory: Path,
                                    completionMessage: String,
                                    createdDirectories: CreatedDirectories)
}
