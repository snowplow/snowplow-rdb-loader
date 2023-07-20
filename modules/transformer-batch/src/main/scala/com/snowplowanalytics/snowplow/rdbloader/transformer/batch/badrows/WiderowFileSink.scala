/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hadoop.io.compress.GzipCodec

import java.io.{BufferedWriter, OutputStream, OutputStreamWriter}
import java.net.URI
import java.nio.charset.StandardCharsets

// Doesn't support shredded directory style as there is no partitioning by vendor/name/model.
// It simply saves files under 'output=bad' directory.
final class WiderowFileSink(
  outputFolder: BlobStorage.Folder,
  hadoopConfiguration: Configuration,
  compression: Compression
) extends BadrowSink {

  override def sink(badrows: List[String], partitionIndex: String): Unit = {
    val outputFile = openFile(partitionIndex)
    val outputStream = createOutputStream(outputFile)
    val writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))

    try
      flushBadrowsToFile(badrows, writer)
    finally
      writer.close()
  }

  private def openFile(
    partitionIndex: String
  ): FSDataOutputStream = {
    val fileName = s"part-$partitionIndex.$outputFileExtension"
    val path = new Path(outputFolder, fileName)
    path.getFileSystem(hadoopConfiguration).create(path, false)
  }

  private def createOutputStream(outputFile: FSDataOutputStream): OutputStream =
    compression match {
      case Compression.None =>
        outputFile
      case Compression.Gzip =>
        val gzipCodec = new GzipCodec()
        gzipCodec.setConf(hadoopConfiguration)
        gzipCodec.createOutputStream(outputFile)
    }

  private def flushBadrowsToFile(badRows: List[String], writer: BufferedWriter): Unit =
    badRows
      .foreach { badRow =>
        writer.write(badRow)
        writer.newLine()
      }

  private def outputFileExtension: String =
    compression match {
      case Compression.None =>
        "txt"
      case Compression.Gzip =>
        "txt.gz"
    }
}
object WiderowFileSink {

  def create(
    folderName: String,
    hadoopConfiguration: Configuration,
    outputPath: URI,
    compression: Compression
  ): WiderowFileSink = {
    val outputFolder = BlobStorage.Folder
      .coerce(outputPath.toString)
      .append(folderName)
      .append("output=bad")

    new WiderowFileSink(outputFolder, hadoopConfiguration, compression)
  }
}
