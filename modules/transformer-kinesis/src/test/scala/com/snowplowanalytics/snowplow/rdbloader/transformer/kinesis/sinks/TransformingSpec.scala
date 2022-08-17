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
package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks

import cats.effect.{Blocker, ContextShift, IO, Timer}

import java.nio.file.Path
import java.time.Instant
import cats.implicits._
import fs2.io.{file => fs2File}
import fs2.{Stream, text}
import io.circe.Json
import io.circe.optics.JsonPath._
import io.circe.parser.{parse => parseCirce}
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.schemaddl.Properties
import com.snowplowanalytics.lrumap.CreateLruMap
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{LookupProperties, PropertiesKey, Transformed}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.{Processing, Transformer}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sources.{Checkpointer, ParsedC, file => FileSource}
import org.specs2.mutable.Specification

class TransformingSpec extends Specification {
  import TransformingSpec._
  import Processing._
  "transform" should {
    "shred events correctly" in {
      val (good, bad) = transformTestEvents(resourcePath = "/processing-spec/1/input/events", format = shredFormat)

      val testFileNameMap = List(
        Transformed.Shredded.Tabular("com.snowplowanalytics.snowplow", "atomic", 1, dummyTransformedData).getPath -> "com.snowplowanalytics.snowplow-atomic",
        Transformed.Shredded.Tabular("com.snowplowanalytics.snowplow", "consent_document", 1, dummyTransformedData).getPath -> "com.snowplowanalytics.snowplow-consent_document",
        Transformed.Shredded.Tabular("com.optimizely", "state", 1, dummyTransformedData).getPath -> "com.optimizely-state"
      ).toMap

      val expectedTransformedMap = getExpectedTransformedEvents(good, testFileNameMap, 1, LoaderMessage.TypesInfo.Shredded.ShreddedFormat.TSV)

      val expectedBadEvent = getResourceLines("/processing-spec/1/output/bad").head

      bad must have size(1)
      bad.head.data.str must beSome(expectedBadEvent)
      good must have size(46)
      // Checks whether transformed events are identical with the expected ones.
      // Only the paths in "pathsToCheck" are compared to not add
      // all shredded events to the test folder.
      good must contain(beLike[(SinkPath, Transformed.Data)] {
        case (path: SinkPath, data) if testFileNameMap.contains(path) =>
          expectedTransformedMap(path) must contain(data.str.get)
        case _ => ok
      }).forall
    }

    "transform events to wide row correctly" in {
      val (good, bad) = transformTestEvents(resourcePath = "/processing-spec/1/input/events", format = wideRowFormat)

      val expectedBadEvent = getResourceLines("/processing-spec/1/output/bad").head
      val expectedGoodEvents = getResourceLines("/processing-spec/1/output/good/widerow/events")

      bad must have size(1)
      bad.head.data.str.get mustEqual expectedBadEvent
      good must have size(2)
      good.map(_.data.str.get) mustEqual expectedGoodEvents
    }

    "create bad row when timestamp is invalid" in {
      val timestampLowerLimit = Instant.parse("0000-01-02T00:00:00.00Z")
      val (good, bad) = transformTestEvents(resourcePath = "/processing-spec/2/input/events", format = wideRowFormat, timestampLowerLimit = Some(timestampLowerLimit))

      val expectedBadEvents = getResourceLines("/processing-spec/2/output/bad")

      bad must have size(2)
      replaceFailureTimestamps(bad.map(_.data.str.get)).toSet mustEqual replaceFailureTimestamps(expectedBadEvents).toSet
      good must have size(1)
    }
  }
}

object TransformingSpec {
  type TransformedList = List[(SinkPath, Transformed.Data)]
  type TransformedMap = Map[SinkPath, List[String]]

  implicit val CS: ContextShift[IO] = IO.contextShift(concurrent.ExecutionContext.global)
  implicit val T: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)

  implicit class TransformedPathClassify(value: (SinkPath, Transformed.Data)) {
    def getBad: Option[(SinkPath, Transformed.Data)] =
      if (value._1.value.contains(BadPathPrefix)) Some(value) else None

    def getGood: Option[(SinkPath, Transformed.Data)] =
      if (getBad.isDefined) None else Some(value)

    def path: SinkPath = value._1

    def data: Transformed.Data = value._2
  }

  val VersionPlaceholder = "version_placeholder"
  val BadPathPrefix = "output=bad"
  val DefaultTimestamp = "2020-09-29T10:38:56.653Z"

  val defaultIgluClient = Client.IgluCentral
  val defaultAtomicLengths: Map[String, Int] = Map.empty
  val wideRowFormat = TransformerConfig.Formats.WideRow.JSON
  val shredFormat = TransformerConfig.Formats.Shred(LoaderMessage.TypesInfo.Shredded.ShreddedFormat.TSV, List.empty, List.empty, List.empty)
  val testBlocker = Blocker.liftExecutionContext(concurrent.ExecutionContext.global)
  val defaultWindow = Window(1, 1, 1, 1, 1)
  val dummyTransformedData = Transformed.Data.DString("")

  def propsLookup: LookupProperties[IO] = CreateLruMap[IO, PropertiesKey, Properties].create(100).unsafeRunSync()

  def createTransformer(formats: TransformerConfig.Formats): Transformer[IO] =
    formats match {
      case f: TransformerConfig.Formats.Shred =>
        Transformer.ShredTransformer(defaultIgluClient, propsLookup, f, defaultAtomicLengths)
      case f: TransformerConfig.Formats.WideRow =>
        Transformer.WideRowTransformer(defaultIgluClient, f)
    }

  def transformTestEvents(resourcePath: String,
                          format: TransformerConfig.Formats,
                          timestampLowerLimit: Option[Instant] = None): (TransformedList, TransformedList) = {
    val transformer = createTransformer(format)
    val validations = TransformerConfig.Validations(timestampLowerLimit)
    implicit val checkpointer = Checkpointer.noOpCheckpointer[IO, Unit]


    val eventStream = parsedEventStream(resourcePath)
      .through(Processing.transform(transformer, validations))
      .through(Processing.handleTransformResult(transformer))

    val transformed = eventStream.compile.toList.unsafeRunSync().flatMap(_._1)
    (transformed.flatMap(_.getGood), transformed.flatMap(_.getBad))
  }

  def parsedEventStream(resourcePath: String): Stream[IO, ParsedC[Unit]] =
    fileStream(resourcePath)
      .map(FileSource.parse)
      .map(p => (p, ()))

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
                                   testFileNameMap: Map[SinkPath, String],
                                   testNumber: Int,
                                   format: LoaderMessage.TypesInfo.Shredded.ShreddedFormat): TransformedMap =
    transformedList.flatMap {
      case (path: SinkPath, _) if testFileNameMap.contains(path) =>
        val testFilePath = s"/processing-spec/${testNumber}/output/good/${format.path}/${testFileNameMap(path)}"
        Some((path, getResourceLines(testFilePath)))
      case _ =>
        None
    }.toMap

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
