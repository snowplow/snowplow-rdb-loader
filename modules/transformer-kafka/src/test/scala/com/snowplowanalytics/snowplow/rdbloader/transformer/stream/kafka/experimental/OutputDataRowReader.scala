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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.experimental

import cats.effect.IO
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.ParquetUtils
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.experimental.TransformerSpecification.{Blob, DataRow}
import fs2.io.file.{Files, Path}
import io.circe.parser

import java.nio.charset.StandardCharsets

object OutputDataRowReader {

  def fromJson(blob: Blob): IO[List[DataRow]] =
    fs2.Stream
      .emits[IO, Byte](blob)
      .through(fs2.text.decodeWithCharset(StandardCharsets.UTF_8))
      .through(fs2.text.lines)
      .map(parser.parse(_).right.get)
      .compile
      .toList

  // For parquet we fetch all bytes from remote blob storage and store them in the temporary local output.
  // Then we use hadoop API (details in the `ParquetUtils`) to decode it and convert to human-readable JSON format.
  def fromParquet(blob: Blob): IO[List[DataRow]] =
    Files[IO].tempFile
      .use { tempOutput =>
        for {
          _ <- saveParquetDataToTemporaryOutput(tempOutput, blob)
          outputParquetColumns = ParquetUtils.readFileColumns(tempOutput.toNioPath.toFile)
          parquetRows <- ParquetUtils.readParquetRowsAsJsonFrom(tempOutput, outputParquetColumns)
        } yield parquetRows
      }

  private def saveParquetDataToTemporaryOutput(outputPath: Path, blob: Blob): IO[Unit] =
    fs2.Stream
      .emits(blob)
      .through(Files[IO].writeAll(outputPath))
      .compile
      .drain

}
