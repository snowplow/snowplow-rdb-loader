package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hadoop.io.compress.GzipCodec

import java.io.{BufferedWriter, OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

object PartitionDataFilter {

  type PartitionData = Iterator[Transformed]
  type GoodData = Iterator[Transformed]
  type BadData = Iterator[Transformed]

  def extractGoodAndPersistBad(
    partitionData: PartitionData,
    partitionIndex: Int,
    folderName: String,
    hadoopConfiguration: Configuration,
    outputConfig: Config.Output
  ): GoodData = {
    val (good, bad) = splitPartitionToGoodAndBad(partitionData)

    if (bad.hasNext) {
      handleBadrowsFromPartition(folderName, hadoopConfiguration, outputConfig, partitionIndex, bad)
    }

    good
  }

  private def splitPartitionToGoodAndBad(data: PartitionData): (GoodData, BadData) =
    data.partition {
      case Transformed.WideRow(isGood, _) => isGood
      case _ => true
    }

  private def handleBadrowsFromPartition(
    folderName: String,
    hadoopConfiguration: Configuration,
    outputConfig: Config.Output,
    partitionIndex: Int,
    badRows: BadData
  ): Unit = {
    val outputFile = openFile(folderName, hadoopConfiguration, outputConfig, partitionIndex)
    val outputStream = createOutputStream(outputFile, hadoopConfiguration, outputConfig)
    val writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))

    try
      flushBadrowsToFile(badRows, writer)
    finally
      writer.close()
  }

  private def openFile(
    folderName: String,
    hadoopConfiguration: Configuration,
    outputConfig: Config.Output,
    partitionIndex: Int
  ): FSDataOutputStream = {
    val extension = getOutputFileExtension(outputConfig)
    val directory = s"${outputConfig.path.toString}/$folderName/output=bad"
    val fileName = s"part-$partitionIndex.$extension"
    val path = new Path(directory, fileName)
    path.getFileSystem(hadoopConfiguration).create(path, false)
  }

  private def createOutputStream(
    outputFile: FSDataOutputStream,
    hadoopConfiguration: Configuration,
    outputConfig: Config.Output
  ): OutputStream =
    outputConfig.compression match {
      case Compression.None =>
        outputFile
      case Compression.Gzip =>
        val gzipCodec = new GzipCodec()
        gzipCodec.setConf(hadoopConfiguration)
        gzipCodec.createOutputStream(outputFile)
    }

  private def flushBadrowsToFile(badRows: BadData, writer: BufferedWriter): Unit =
    badRows
      .collect { case Transformed.WideRow(false, dString) => dString.value }
      .foreach { badRow =>
        writer.write(badRow)
        writer.newLine()
      }

  private def getOutputFileExtension(outputConfig: Config.Output): String =
    outputConfig.compression match {
      case Compression.None =>
        "txt"
      case Compression.Gzip =>
        "txt.gz"
    }

}
