package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import com.amazonaws.services.kinesis.model.{PutRecordsRequest, PutRecordsResult}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data.DString
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.WideRow
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.PartitionDataFilterSpec.{MockedKinesisStream, TempDirectory}
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows.{BadrowSink, FileSink, KinesisSink, PartitionDataFilter}
import org.apache.hadoop.io.compress.GzipCodec
import org.specs2.execute.{AsResult, Result}
import org.specs2.mutable.Specification
import org.specs2.specification.ForEach

import java.io.{File, FileInputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import scala.collection.JavaConverters._
import scala.io.Source

class PartitionDataFilterSpec extends Specification with SparkSpec with TempDirectory {

  "Good data should be preserved in output and bad data sinked when partition contains" >> {
    "only good data, output to uncompressed file" in { (tempOutput: Path) =>
      val partitionData = List(
        good("good content1"),
        good("good content2")
      )

      val sink = fileSink(Compression.None, tempOutput)
      val goodOutput = extractGoodAndSinkBad(partitionData, sink)

      goodOutput must beEqualTo(List("good content1", "good content2"))
      Files.exists(Paths.get(tempOutput.toString, "run=1970-01-01-00-00-00/output=bad")) must beEqualTo(false)
    }

    "only bad data, output to uncompressed file" in { (tempOutput: Path) =>
      val partitionData = List(
        bad("bad content1"),
        bad("bad content2")
      )

      val sink = fileSink(Compression.None, tempOutput)
      val goodOutput = extractGoodAndSinkBad(partitionData, sink)

      listFilesIn(tempOutput, folderPath = "run=1970-01-01-00-00-00/output=bad").head must beEqualTo("part-1.txt")
      val badFromFile = readUncompressedBadrows(tempOutput, filePath = s"run=1970-01-01-00-00-00/output=bad/part-1.txt")

      goodOutput must beEqualTo(List.empty)
      badFromFile must beEqualTo(List("bad content1", "bad content2"))
    }

    "good and bad data, output to uncompressed file" in { (tempOutput: Path) =>
      val partitionData = List(
        good("good content1"),
        bad("bad content1"),
        good("good content2")
      )

      val sink = fileSink(Compression.None, tempOutput)
      val goodOutput = extractGoodAndSinkBad(partitionData, sink)

      listFilesIn(tempOutput, folderPath = "run=1970-01-01-00-00-00/output=bad").head must beEqualTo("part-1.txt")

      val badFromFile = readUncompressedBadrows(tempOutput, filePath = s"run=1970-01-01-00-00-00/output=bad/part-1.txt")

      goodOutput must beEqualTo(List("good content1", "good content2"))
      badFromFile must beEqualTo(List("bad content1"))
    }
    "good and bad data, output to compressed file" in { (tempOutput: Path) =>
      val partitionData = List(
        good("good content1"),
        bad("bad content1"),
        good("good content2")
      )

      val sink = fileSink(Compression.Gzip, tempOutput)
      val goodOutput = extractGoodAndSinkBad(partitionData, sink)

      listFilesIn(tempOutput, folderPath = "run=1970-01-01-00-00-00/output=bad").head must beEqualTo("part-1.txt.gz")

      val badFromFile = readCompressedBadrows(tempOutput, filePath = s"run=1970-01-01-00-00-00/output=bad/part-1.txt.gz")

      goodOutput must beEqualTo(List("good content1", "good content2"))
      badFromFile must beEqualTo(List("bad content1"))
    }

    "good and bad data, output to kinesis" in {
      val partitionData = List(
        good("good content1"),
        bad("bad content1"),
        good("good content2")
      )

      val stream = new MockedKinesisStream
      val sink = kinesisSink(stream, streamName = "mockedStream")
      val goodOutput = extractGoodAndSinkBad(partitionData, sink)

      val kinesisRequest = stream.receivedPutRequests.toList.head
      val kinesisData = new String(kinesisRequest.getRecords.asScala.head.getData.array(), UTF_8)

      goodOutput must beEqualTo(List("good content1", "good content2"))
      kinesisRequest.getStreamName must beEqualTo("mockedStream")
      kinesisData must beEqualTo("bad content1")
    }
  }

  private def extractGoodAndSinkBad(
    input: List[Transformed],
    sink: BadrowSink
  ) =
    PartitionDataFilter
      .extractGoodAndSinkBad(
        input.iterator,
        partitionIndex = 1,
        sink
      )
      .collect { case d: WideRow => d.data.value }
      .toList

  private def fileSink(compression: Compression, badRowsRoot: Path) =
    new FileSink(
      folderName = "run=1970-01-01-00-00-00",
      hadoopConfiguration = spark.sparkContext.hadoopConfiguration,
      outputPath = badRowsRoot.toUri,
      compression = compression
    )

  private def kinesisSink(kinesisStream: MockedKinesisStream, streamName: String) = {
    val flushDataToKinesis = kinesisStream.receive _
    new KinesisSink(flushDataToKinesis, streamName, recordLimit = 2, byteLimit = 100)
  }

  private def readUncompressedBadrows(badRowsRoot: Path, filePath: String) = {
    val outputFile = Paths.get(badRowsRoot.toString, filePath)
    val source = Source.fromFile(outputFile.toUri)
    source.getLines().toList
  }

  private def readCompressedBadrows(badRowsRoot: Path, filePath: String) = {
    val codec = new GzipCodec()
    codec.setConf(spark.sparkContext.hadoopConfiguration)

    val stream = codec.createInputStream(new FileInputStream(Paths.get(badRowsRoot.toString, filePath).toFile))
    Source.createBufferedSource(stream).getLines().toList
  }

  private def listFilesIn(badRowsRoot: Path, folderPath: String) =
    Files
      .list(Paths.get(badRowsRoot.toString, folderPath))
      .iterator()
      .asScala
      .toList
      .filterNot(_.toFile.isHidden)
      .map(_.getFileName.toString)

  private def good(content: String) = WideRow(good = true, DString(content))
  private def bad(content: String) = WideRow(good = false, DString(content))

  override def appName: String = "badrows-test"
}

object PartitionDataFilterSpec {

  import scala.collection.mutable

  class MockedKinesisStream {
    val receivedPutRequests: mutable.ListBuffer[PutRecordsRequest] = mutable.ListBuffer.empty

    def receive(request: PutRecordsRequest): PutRecordsResult = {
      receivedPutRequests += request
      new PutRecordsResult().withFailedRecordCount(0)
    }
  }

  trait TempDirectory extends ForEach[Path] {
    override protected def foreach[R: AsResult](f: Path => R): Result = {
      val tempDirectory = Files.createTempDirectory("badrows-temp")

      try AsResult[R](f(tempDirectory))
      finally deleteRecursively(tempDirectory.toFile)
    }

    private def deleteRecursively(file: File): Unit =
      if (file.isDirectory) {
        file.listFiles.foreach(deleteRecursively)
      } else if (file.exists && !file.delete) {
        throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
      }
  }
}
