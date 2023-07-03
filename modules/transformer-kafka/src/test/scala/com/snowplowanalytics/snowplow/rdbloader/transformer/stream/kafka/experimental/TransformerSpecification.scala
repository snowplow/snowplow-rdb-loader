package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.experimental

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow.WideRowFormat
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.{BlobObject, Folder}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.experimental.TransformerSpecification._
import fs2.Stream
import fs2.compression.{Compression => FS2Compression}
import fs2.concurrent.Signal.SignalOps
import fs2.concurrent.{Signal, SignallingRef}
import io.circe.Json
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.DurationInt

// format: off
/**
 * Generic template for the streaming transformer specification. It produces specified input (could be delayed batches), 
 * waits for incoming windows (can be more than one) with data and asserts produced by the transformer output. 
 * It assumes transformer app is already running somewhere with accessible input/output.
 * It verifies facts about transformer which should be true for any kind of transformation.
 * 
 * It's an abstract class and can't be run on its own. Specific implementation needs to implement abstract members like:
 * - 'description' - details about scenario
 * - 'inputBatches' - list of input events. Could be any kind of good/bad data in the TSV format. Delivery of a batch can be delayed if any delay is specified.
 * - 'countExpectations' - how many good/bad events are expected on the output. If we reach expected count, test is interrupted and assertions are run.
 * - 'requiredAppConfig' - expected configuration details for tested transformer application
 * - 'customDataAssertion' - optional, allows to execute more detailed assertions of an output, like checking  values for transformed events.  
 * 
 * Dependencies like blob storage client and queue consumer/producer need to be supplied by a proper `AppDependencies.Provider` implementation.
 * 
 * What kind of assertions are run:
 * - `shredding_complete.json` exists in the storage
 * - `shredding_complete.json` consumed from the queue is equal to the one in the storage
 * - `shredding_complete.json` count values matches the number of actual output events (good and bad) in the storage (for each window)
 * - expected output format is used
 * - expected output compression is used
 * - windows are emitted with expected frequency
 */
// format: on

abstract class TransformerSpecification extends Specification with AppDependencies.Provider {

  private val timeout = 15.minutes

  protected def description: String
  protected def inputBatches: List[InputBatch]
  protected def countExpectations: CountExpectations
  protected def requiredAppConfig: AppConfiguration
  protected def customDataAssertion: Option[DataAssertion] = None

  s"$description" in {
    run().unsafeRunSync()
  }

  def run(): IO[MatchResult[Any]] =
    createDependencies()
      .use { dependencies =>
        for {
          windowsAccumulator <- SignallingRef.of[IO, WindowsAccumulator](WindowsAccumulator(List.empty))
          consuming = consumeAllIncomingWindows(dependencies, countExpectations, windowsAccumulator)
          producing = produceInput(inputBatches, dependencies)
          _ <- consuming.concurrently(producing).compile.drain
          collectedWindows <- windowsAccumulator.get
        } yield {
          collectedWindows.value.foreach(assertSingleWindow)
          assertWindowingFrequency(collectedWindows.value)

          val aggregatedData = aggregateDataFromAllWindows(collectedWindows.value)
          assertAggregatedCounts(aggregatedData)
          customDataAssertion.fold(ok)(_.apply(aggregatedData))
        }
      }

  private def produceInput(batches: List[InputBatch], dependencies: AppDependencies): Stream[IO, Unit] =
    Stream.eval {
      IO.sleep(10.seconds) *> batches.traverse_ { batch =>
        IO.sleep(batch.delay) *> InputBatch
          .asTextLines(batch.content)
          .parTraverse_(dependencies.producer.send)
      }
    }

  private def consumeAllIncomingWindows(
    dependencies: AppDependencies,
    countExpectations: CountExpectations,
    windowsAccumulator: SignallingRef[IO, WindowsAccumulator]
  ): Stream[IO, Unit] =
    dependencies.queueConsumer.read
      .map(_.content)
      .map(parseShreddingCompleteMessage)
      .evalMap(readWindowOutput(dependencies.blobClient))
      .evalMap { windowOutput =>
        windowsAccumulator.update(_.addWindow(windowOutput))
      }
      .interruptWhen(allEventsProcessed(windowsAccumulator, countExpectations))
      .interruptAfter(timeout)

  private def assertSingleWindow(output: WindowOutput): MatchResult[Any] = {
    output.`shredding_complete.json`.compression must beEqualTo(requiredAppConfig.compression)
    output.`shredding_complete.json`.count.get.good must beEqualTo(output.goodEvents.size)
    output.`shredding_complete.json`.count.get.bad.get must beEqualTo(output.badEvents.size)
    output.`shredding_complete.json`.typesInfo must beLike { case TypesInfo.WideRow(fileFormat, _) =>
      fileFormat must beEqualTo(requiredAppConfig.fileFormat)
    }
  }

  private def assertWindowingFrequency(collectedWindows: List[WindowOutput]): Unit =
    collectedWindows.groupBy(_.appId).foreach { case (_, windows) =>
      windows.zip(windows.tail).foreach { case (window1, window2) =>
        ChronoUnit.MINUTES.between(window1.producedAt, window2.producedAt) must beEqualTo(requiredAppConfig.windowFrequencyMinutes)
      }
    }

  private def assertAggregatedCounts(aggregatedData: AggregatedData): MatchResult[Any] = {
    aggregatedData.good.size must beEqualTo(countExpectations.good)
    aggregatedData.bad.size must beEqualTo(countExpectations.bad)
  }

  private def aggregateDataFromAllWindows(windows: List[WindowOutput]): AggregatedData =
    windows.foldLeft(AggregatedData.empty) { case (aggregated, window) =>
      val windowTypes = window.`shredding_complete.json`.typesInfo.asInstanceOf[TypesInfo.WideRow].types
      AggregatedData(
        good = window.goodEvents ::: aggregated.good,
        bad = window.badEvents ::: aggregated.bad,
        types = windowTypes ::: aggregated.types
      )
    }

  private def allEventsProcessed(
    windowsAccumulator: SignallingRef[IO, WindowsAccumulator],
    countExpectations: CountExpectations
  ): Signal[IO, Boolean] =
    windowsAccumulator
      .map(_.getTotalNumberOfEvents >= countExpectations.total)

  private def readWindowOutput(blobClient: BlobStorage[IO])(message: LoaderMessage.ShreddingComplete): IO[WindowOutput] =
    for {
      scMessageInStorage <- readSCMessageFromBlobStorage(message, blobClient)
      transformedEvents <- readDataRowsFromFolder(scMessageInStorage.base.append("output=good"), blobClient)
      badEvents <- readDataRowsFromFolder(scMessageInStorage.base.append("output=bad"), blobClient)
    } yield {

      message must beEqualTo(scMessageInStorage)

      // Assuming folder name structure ending like '.../run=yyyy-MM-dd-HH-mm-ss-${UUID}/'
      val base = message.base.stripSuffix("/")
      val appID = base.takeRight(36) // extract application ID which is represented by UUID at the end of a folder name
      val time = base.stripSuffix(appID).stripSuffix("-").takeRight(19) // extract date and time of the window, but without `run=` prefix
      val parsedTime = LocalDateTime.parse(time, DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss"))

      println(
        s"""Received `shredding_complete.json` message:
           |- folder - ${message.base}
           |- counts - ${message.count}
           |""".stripMargin
      )

      WindowOutput(appID, parsedTime, message, transformedEvents, badEvents)
    }

  private def readDataRowsFromFolder(
    folder: Folder,
    blobClient: BlobStorage[IO]
  ): IO[List[DataRow]] =
    blobClient
      .list(folder, recursive = false)
      .evalMap(blob => readDataRowsFromBlob(blobClient, blob))
      .flatMap(dataRows => Stream.emits(dataRows))
      .compile
      .toList

  private def readDataRowsFromBlob(
    blobClient: BlobStorage[IO],
    blob: BlobObject
  ): IO[List[DataRow]] =
    blobClient
      .getBytes(blob.key)
      .through(decompressIfNeeded(blob.key))
      .compile
      .to(Array)
      .flatMap(convertBlobToListOfRows(blob.key))

  private def convertBlobToListOfRows(blobKey: BlobStorage.Key)(blob: Blob): IO[List[DataRow]] =
    if (isBlobGoodParquetData(blobKey))
      OutputDataRowReader.fromParquet(blob)
    else
      OutputDataRowReader.fromJson(blob)

  // Decompress only for:
  // - JSON good/bad output
  // - Parquet bad output
  // Decompression for parquet good data is handled by hadoop API in parquet-oriented scenarios
  private def decompressIfNeeded(blobKey: BlobStorage.Key): fs2.Pipe[IO, Byte, Byte] =
    requiredAppConfig.compression match {
      case Compression.Gzip if !isBlobGoodParquetData(blobKey) =>
        FS2Compression[IO].gunzip().andThen(_.flatMap(_.content))
      case _ =>
        identity
    }

  private def isBlobGoodParquetData(blobKey: BlobStorage.Key): Boolean =
    requiredAppConfig.fileFormat == WideRowFormat.PARQUET && blobKey.contains("output=good")

  private def readSCMessageFromBlobStorage(
    message: LoaderMessage.ShreddingComplete,
    blobClient: BlobStorage[IO]
  ): IO[LoaderMessage.ShreddingComplete] =
    scMessageMustExist(message, blobClient) *> fetchSCMessage(message, blobClient)

  private def scMessageMustExist(message: LoaderMessage.ShreddingComplete, blobClient: BlobStorage[IO]) =
    blobClient
      .keyExists(message.base.withKey("shredding_complete.json"))
      .map(messageExists => messageExists must beTrue)

  private def fetchSCMessage(message: LoaderMessage.ShreddingComplete, blobClient: BlobStorage[IO]): IO[LoaderMessage.ShreddingComplete] =
    blobClient
      .get(message.base.withKey("shredding_complete.json"))
      .map(value => parseShreddingCompleteMessage(value.right.get))

  private def parseShreddingCompleteMessage(message: String): LoaderMessage.ShreddingComplete =
    LoaderMessage.fromString(message) match {
      case Right(parsedMessage: LoaderMessage.ShreddingComplete) => parsedMessage
      case other => throw new IllegalStateException(s"Provided message is not a valid shredding complete message - $other")
    }

}

object TransformerSpecification {

  type Blob = Array[Byte]
  type DataRow = Json
  type DataAssertion = AggregatedData => MatchResult[Any]

  final case class CountExpectations(good: Int, bad: Int) {
    def total = good + bad
  }

  final case class WindowsAccumulator(value: List[WindowOutput]) {
    def addWindow(window: WindowOutput): WindowsAccumulator =
      WindowsAccumulator(value :+ window)

    def getTotalNumberOfEvents: Long =
      value.map { window =>
        val good = window.`shredding_complete.json`.count.map(_.good).getOrElse(0L)
        val bad = window.`shredding_complete.json`.count.flatMap(_.bad).getOrElse(0L)
        good + bad
      }.sum
  }

  final case class WindowOutput(
    appId: String,
    producedAt: LocalDateTime,
    `shredding_complete.json`: LoaderMessage.ShreddingComplete,
    goodEvents: List[DataRow],
    badEvents: List[DataRow]
  )

  final case class AggregatedData(
    good: List[DataRow],
    bad: List[DataRow],
    types: List[TypesInfo.WideRow.Type]
  )

  object AggregatedData {
    val empty = AggregatedData(List.empty, List.empty, List.empty)
  }
}
