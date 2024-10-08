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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.WiderowFileSinkSpec.TempDirectory
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows.WiderowFileSink
import org.apache.hadoop.io.compress.GzipCodec
import org.specs2.execute.{AsResult, Result}
import org.specs2.mutable.Specification
import org.specs2.specification.ForEach

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Path, Paths}
import scala.collection.JavaConverters._
import scala.io.Source

class WiderowFileSinkSpec extends Specification with SparkSpec with TempDirectory {

  "File sink should work correctly for transformed widerow badrow data when" >> {
    "output to uncompressed file" in { (tempOutput: Path) =>
      val partitionData = List(
        "bad1",
        "bad2"
      )

      fileSink(Compression.None, tempOutput).sink(partitionData, partitionIndex = "1")
      listFilesIn(tempOutput, folderPath = "run=1970-01-01-00-00-00/output=bad").head must beEqualTo("part-1.txt")

      val badFromFile = readUncompressedBadrows(tempOutput, filePath = s"run=1970-01-01-00-00-00/output=bad/part-1.txt")
      badFromFile must beEqualTo(List("bad1", "bad2"))
    }

    "output to compressed file" in { (tempOutput: Path) =>
      val partitionData = List(
        "bad1",
        "bad2"
      )

      fileSink(Compression.Gzip, tempOutput).sink(partitionData, partitionIndex = "1")
      listFilesIn(tempOutput, folderPath = "run=1970-01-01-00-00-00/output=bad").head must beEqualTo("part-1.txt.gz")

      val badFromFile = readCompressedBadrows(tempOutput, filePath = s"run=1970-01-01-00-00-00/output=bad/part-1.txt.gz")
      badFromFile must beEqualTo(List("bad1", "bad2"))
    }
  }

  private def fileSink(compression: Compression, badRowsRoot: Path) =
    WiderowFileSink.create(
      folderName          = "run=1970-01-01-00-00-00",
      hadoopConfiguration = spark.sparkContext.hadoopConfiguration,
      outputPath          = badRowsRoot.toUri,
      compression         = compression
    )

  private def readUncompressedBadrows(badRowsRoot: Path, filePath: String) = {
    val outputFile = Paths.get(badRowsRoot.toString, filePath)
    val source     = Source.fromFile(outputFile.toUri)
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

  override def appName: String = "widerow-file-sink-test"
}

object WiderowFileSinkSpec {
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
