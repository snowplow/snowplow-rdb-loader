package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import com.snowplowanalytics.snowplow.rdbloader.common.config.Region
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data.DString
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.WideRow
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.PartitionDataFilterSpec.TempDirectory
import org.apache.hadoop.io.compress.GzipCodec
import org.specs2.execute.{AsResult, Result}
import org.specs2.mutable.Specification
import org.specs2.specification.ForEach

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Path, Paths}
import scala.io.Source
import scala.collection.JavaConverters._

class PartitionDataFilterSpec extends Specification with SparkSpec with TempDirectory {

  "Good data should be preserved in output and bad data should be flushed to filesystem when partition contains" >> {
    "only good data" in { (tempOutput: Path) =>
      val partitionData = List(
        good("good content1"),
        good("good content2")
      )

      val goodOutput = extractGoodAndPersistBad(partitionData, Compression.None, tempOutput)

      goodOutput must beEqualTo(List("good content1", "good content2"))
      Files.exists(Paths.get(tempOutput.toString, "run=1970-01-01-00-00-00/output=bad")) must beEqualTo(false)
    }

    "only bad data" in { (tempOutput: Path) =>
      val partitionData = List(
        bad("bad content1"),
        bad("bad content2")
      )

      val goodOutput = extractGoodAndPersistBad(partitionData, Compression.None, tempOutput)

      listFilesIn(tempOutput, folderPath = "run=1970-01-01-00-00-00/output=bad") must beEqualTo(List("part-1.txt"))
      val flushedBad = readUncompressedBadrows(tempOutput, filePath = s"run=1970-01-01-00-00-00/output=bad/part-1.txt")

      goodOutput must beEqualTo(List.empty)
      flushedBad must beEqualTo(List("bad content1", "bad content2"))
    }

    "both good and bad data" in { (tempOutput: Path) =>
      val partitionData = List(
        good("good content1"),
        bad("bad content1"),
        good("good content2")
      )

      val goodOutput = extractGoodAndPersistBad(partitionData, Compression.None, tempOutput)

      listFilesIn(tempOutput, folderPath = "run=1970-01-01-00-00-00/output=bad") must beEqualTo(List("part-1.txt"))
      val flushedBad = readUncompressedBadrows(tempOutput, filePath = s"run=1970-01-01-00-00-00/output=bad/part-1.txt")

      goodOutput must beEqualTo(List("good content1", "good content2"))
      flushedBad must beEqualTo(List("bad content1"))
    }

    "both good and bad data but output file should be compressed" in { (tempOutput: Path) =>
      val partitionData = List(
        good("good content1"),
        bad("bad content1"),
        good("good content2")
      )

      val goodOutput = extractGoodAndPersistBad(partitionData, Compression.Gzip, tempOutput)

      listFilesIn(tempOutput, folderPath = "run=1970-01-01-00-00-00/output=bad") must beEqualTo(List("part-1.txt.gz"))
      val flushedBad = readCompressedBadrows(tempOutput, filePath = s"run=1970-01-01-00-00-00/output=bad/part-1.txt.gz")

      goodOutput must beEqualTo(List("good content1", "good content2"))
      flushedBad must beEqualTo(List("bad content1"))
    }
  }

  private def extractGoodAndPersistBad(
    input: List[Transformed],
    compression: Compression,
    badRowsRoot: Path
  ) = {
    val goodOutput = PartitionDataFilter.extractGoodAndPersistBad(
      input.iterator,
      partitionIndex = 1,
      folderName = "run=1970-01-01-00-00-00",
      spark.sparkContext.hadoopConfiguration,
      Config.Output(badRowsRoot.toUri, compression, Region("none"), 0)
    )

    goodOutput.collect { case d: WideRow => d.data.value }.toList
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
