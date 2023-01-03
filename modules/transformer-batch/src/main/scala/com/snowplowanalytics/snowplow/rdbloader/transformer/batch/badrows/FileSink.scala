package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows

import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hadoop.io.compress.GzipCodec

import java.io.{BufferedWriter, OutputStream, OutputStreamWriter}
import java.net.URI
import java.nio.charset.StandardCharsets

final class FileSink(
  folderName: String,
  hadoopConfiguration: Configuration,
  outputPath: URI,
  compression: Compression
) extends BadrowSink {

  override def sink(badrows: Iterator[String], partitionIndex: Int): Unit = {
    val outputFile = openFile(folderName, hadoopConfiguration, outputPath, compression, partitionIndex)
    val outputStream = createOutputStream(outputFile, hadoopConfiguration, compression)
    val writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))

    try
      flushBadrowsToFile(badrows, writer)
    finally
      writer.close()
  }

  private def openFile(
    folderName: String,
    hadoopConfiguration: Configuration,
    outputPath: URI,
    compression: Compression,
    partitionIndex: Int
  ): FSDataOutputStream = {
    val extension = getOutputFileExtension(compression)
    val directory = s"${outputPath.toString}/$folderName/output=bad"
    val fileName = s"part-$partitionIndex.$extension"
    println(fileName)
    val path = new Path(directory, fileName)
    path.getFileSystem(hadoopConfiguration).create(path, false)
  }

  private def createOutputStream(
    outputFile: FSDataOutputStream,
    hadoopConfiguration: Configuration,
    compression: Compression
  ): OutputStream =
    compression match {
      case Compression.None =>
        outputFile
      case Compression.Gzip =>
        val gzipCodec = new GzipCodec()
        gzipCodec.setConf(hadoopConfiguration)
        gzipCodec.createOutputStream(outputFile)
    }

  private def flushBadrowsToFile(badRows: Iterator[String], writer: BufferedWriter): Unit =
    badRows
      .foreach { badRow =>
        writer.write(badRow)
        writer.newLine()
      }

  private def getOutputFileExtension(compression: Compression): String =
    compression match {
      case Compression.None =>
        "txt"
      case Compression.Gzip =>
        "txt.gz"
    }
}
