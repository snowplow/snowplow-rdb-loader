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
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.FileUtils
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.FileUtils.{createTempDirectory, directoryStream}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.processing.BaseProcessingSpec.{ProcessingOutput, TransformerConfig}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sources.ParsedF
import fs2.Stream
import org.specs2.mutable.Specification

import java.nio.file.Path
import java.util.Base64
import java.util.concurrent.TimeUnit

trait BaseProcessingSpec extends Specification {

  implicit val CS: ContextShift[IO] = IO.contextShift(concurrent.ExecutionContext.global)
  implicit val T: Timer[IO]         = IO.timer(concurrent.ExecutionContext.global)

  //returns always 1970-01-01-10:30
  implicit val clock: Clock[IO] = new Clock[IO] {
    override def realTime(unit: TimeUnit): IO[Long] = IO(unit.convert(37800L, TimeUnit.SECONDS)) 
    override def monotonic(unit: TimeUnit): IO[Long] = IO(unit.convert(37800L, TimeUnit.SECONDS))
  }
  
  val blocker = Blocker.liftExecutionContext(concurrent.ExecutionContext.global)
  protected val temporaryDirectory = createTempDirectory(blocker)

  protected def process(input: Stream[IO, ParsedF[IO, IO[Unit]]],
                        config: TransformerConfig): IO[ProcessingOutput] = {
      val args = prepareAppArgs(config)

      for {
        waitingForCompletionMessage <- Deferred[IO, String]
        runningApp                  <- TestApplication.run(args, waitingForCompletionMessage, input).start
        completionMessage           <- waitingForCompletionMessage.get
        _                           <- runningApp.cancel
      } yield ProcessingOutput(completionMessage) 
    }

  protected def assertStringRows(actualRows: List[String],
                                 expectedRows: List[String]) = {
    actualRows.zip(expectedRows).map {
      case (actual, expected) => actual must beEqualTo(expected)
    }
      .reduce(_ and _)
  }

  protected def readMessageFromResource(resource: String,
                                        outputRootDirectory: Path) = {
   readLinesFromResource(resource) 
      .map(_.mkString)
      .map(
        _
          .replace("output_path_placeholder", outputRootDirectory.toUri.toString.replaceAll("/+$", ""))
          .replace("version_placeholder", BuildInfo.version)
          .replace(" ", "")
      )
  }

  protected def readStringRowsFrom(path: Path): IO[List[String]] = {
    directoryStream(blocker, path)
      .compile
      .toList
  }

  protected def readLinesFromResource(resource: String) = {
    FileUtils.readLines(blocker, resource)
  }

  private def prepareAppArgs(config: TransformerConfig) = {
    val encoder = Base64.getUrlEncoder

    List(
      "--iglu-config", new String(encoder.encode(config.iglu.getBytes)),
      "--config", new String(encoder.encode(config.app.getBytes))
    )
  }
}

object BaseProcessingSpec {

  final case class TransformerConfig(app: String, iglu: String)
  final case class ProcessingOutput(completionMessage: String)
}
