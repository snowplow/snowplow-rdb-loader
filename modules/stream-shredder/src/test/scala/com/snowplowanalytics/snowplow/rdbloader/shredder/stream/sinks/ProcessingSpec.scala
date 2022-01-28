/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks

import java.nio.file.Path

import cats.implicits._

import cats.effect.{Blocker, ContextShift, IO, Timer}

import fs2.io.{file => fs2File}
import fs2.{Stream, text}

import io.circe.Json
import io.circe.optics.JsonPath._
import io.circe.parser.{ parse => parseCirce }

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.shredder.stream.{Processing, Transformer}
import com.snowplowanalytics.snowplow.rdbloader.shredder.stream.Processing.Windowed
import com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sources.{Parsed, file => FileSource}
import com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks.generic.Record
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo

import org.specs2.mutable.Specification

class ProcessingSpec extends Specification {
  import ProcessingSpec._
  "transform" should {
    "shred events correctly" in {
      val (good, bad) = transformTestEvents(resourcePath = "/processing-spec/1/input/events", format = shredFormat)

      val pathsToCheck = List(
        Transformed.Path.Shredded.Tabular("com.snowplowanalytics.snowplow", "atomic", 1),
        Transformed.Path.Shredded.Tabular("com.snowplowanalytics.snowplow", "consent_document", 1),
        Transformed.Path.Shredded.Tabular("com.optimizely", "state", 1)
      )
      val expectedTransformedMap = getExpectedTransformedEvents(good, pathsToCheck, 1, LoaderMessage.TypesInfo.Shredded.ShreddedFormat.TSV)

      val expectedBadEvent = getResourceLines("/processing-spec/1/output/bad").head

      bad must have size(1)
      bad.head.data.value mustEqual expectedBadEvent
      good must have size(46)
      // Checks whether transformed events are identical with the expected ones.
      // Only the paths in "pathsToCheck" are compared to not add
      // all shredded events to the test folder.
      good must contain(beLike[(Transformed.Path, Transformed.Data)] {
        case (path: Transformed.Path.Shredded, data) if pathsToCheck.contains(path) =>
          expectedTransformedMap(path) must contain(data.value)
        case (_: Transformed.Path.Shredded, _) => ok
      }).forall
    }

    "transform events to wide row correctly" in {
      val (good, bad) = transformTestEvents(resourcePath = "/processing-spec/1/input/events", format = wideRowFormat)

      val expectedBadEvent = getResourceLines("/processing-spec/1/output/bad").head
      val expectedGoodEvents = getResourceLines("/processing-spec/1/output/good/widerow/events")

      bad must have size(1)
      bad.head.data.value mustEqual expectedBadEvent
      good must have size(2)
      good.map(_.data.value) mustEqual expectedGoodEvents
    }
  }
}

object ProcessingSpec {
  type TransformedList = List[(Transformed.Path, Transformed.Data)]
  type TransformedMap = Map[Transformed.Path.Shredded, List[String]]

  implicit val CS: ContextShift[IO] = IO.contextShift(concurrent.ExecutionContext.global)
  implicit val T: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)

  implicit class TransformedPathClassify(value: (Transformed.Path, Transformed.Data)) {
    def getBad: Option[(Transformed.Path, Transformed.Data)] =
      if (value.path.getDir.contains(BadPathPrefix)) Some(value) else None

    def getGood: Option[(Transformed.Path, Transformed.Data)] =
      if (getBad.isDefined) None else Some(value)

    def path: Transformed.Path = value._1

    def data: Transformed.Data = value._2
  }

  val VersionPlaceholder = "version_placeholder"
  val BadPathPrefix = "output=bad"
  val DefaultTimestamp = "2020-09-29T10:38:56.653Z"

  val defaultIgluClient = Client.IgluCentral
  val defaultAtomicLengths: Map[String, Int] = Map.empty
  val wideRowFormat = ShredderConfig.Formats.WideRow.JSON
  val shredFormat = ShredderConfig.Formats.Shred(LoaderMessage.TypesInfo.Shredded.ShreddedFormat.TSV, List.empty, List.empty, List.empty)
  val testBlocker = Blocker.liftExecutionContext(concurrent.ExecutionContext.global)
  val defaultWindow = Window(1, 1, 1, 1, 1)

  def createTransformer(formats: ShredderConfig.Formats): Transformer[IO] =
    formats match {
      case f: ShredderConfig.Formats.Shred =>
        Transformer.ShredTransformer(defaultIgluClient, f, defaultAtomicLengths)
      case f: ShredderConfig.Formats.WideRow =>
        Transformer.WideRowTransformer(f)
    }

  def transformTestEvents(resourcePath: String,
                          format: ShredderConfig.Formats): (TransformedList, TransformedList) = {
    val transformer = createTransformer(format)
    val eventStream: Stream[IO, Windowed[IO, Parsed]] = parsedEventStream(resourcePath)
    val pipe = Processing.transform[IO](transformer)
    val transformed = pipe(eventStream).compile.toList.unsafeRunSync().flatMap {
      case Record.Data(_, _, i) => Some(i)
      case Record.EndWindow(_, _, _) => None
    }
    (transformed.flatMap(_.getGood), transformed.flatMap(_.getBad))
  }

  def parsedEventStream(resourcePath: String): Stream[IO, Windowed[IO, Parsed]] =
    fileStream(resourcePath)
      .map(FileSource.parse)
      .map(Record.Data[IO, Window, Parsed](defaultWindow, Option.empty, _))

  def getResourceLines(resourcePath: String): List[String] =
    fileStream(resourcePath)
      .map(_.replace(VersionPlaceholder, BuildInfo.version))
      .compile
      .toList
      .unsafeRunSync()

  def fileStream(resourcePath: String): Stream[IO, String] = {
    val path = Path.of(getClass.getResource(resourcePath).getPath)
    fs2File.readAll[IO](path, testBlocker, 4096)
      .through(text.utf8Decode)
      .through(text.lines)
      .filter(_.nonEmpty)
  }

  def getExpectedTransformedEvents(transformedList: TransformedList,
                                   pathsToCheck: List[Transformed.Path.Shredded],
                                   testNumber: Int,
                                   format: LoaderMessage.TypesInfo.Shredded.ShreddedFormat): TransformedMap =
    transformedList.flatMap {
      case (path: Transformed.Path.Shredded, _) if pathsToCheck.contains(path) =>
        Some(getResourceForShreddedPath(s"/processing-spec/${testNumber}/output/good/${format.path}", path))
      case _ =>
        None
    }.toMap

  def getResourceForShreddedPath(initPath: String, shreddedPath: Transformed.Path.Shredded): (Transformed.Path.Shredded, List[String]) =
    (shreddedPath, getResourceLines(s"$initPath/${shreddedPath.vendor}-${shreddedPath.name}"))

  val replaceFailureTimestamp: Json => Json =
    root.data.failure.timestamp.string.set(DefaultTimestamp)

  def replaceFailureTimestamps(jsons: List[String]): List[String] =
    jsons
      .map(parseCirce)
      .sequence.map(_.map(replaceFailureTimestamp))
      .toOption
      .get
      .map(_.noSpaces)
}
