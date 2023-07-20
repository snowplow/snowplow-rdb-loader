/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrameWriter, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression

object Sink {

  def writeShredded(
    spark: SparkSession,
    compression: Compression,
    data: RDD[(String, String, String, String, Int, String)],
    outFolder: String
  ): Unit = {
    import spark.implicits._
    data
      .toDF("output", "vendor", "name", "format", "model", "data")
      .write
      .withCompression(compression)
      .partitionBy("output", "vendor", "name", "format", "model")
      .mode(SaveMode.Append)
      .text(outFolder)
  }

  def writeWideRowed(
    spark: SparkSession,
    compression: Compression,
    data: RDD[(String, String)],
    outFolder: String
  ): Unit = {
    import spark.implicits._
    data
      .toDF("output", "data")
      .write
      .withCompression(compression)
      .partitionBy("output")
      .mode(SaveMode.Append)
      .text(outFolder)
  }

  def writeParquet(
    spark: SparkSession,
    sparkSchema: StructType,
    data: RDD[List[Any]],
    outFolder: String,
    maxRecordsPerFile: Long
  ): Unit = {
    val rows = data.map(Row.fromSeq)
    spark
      .createDataFrame(rows, sparkSchema)
      .write
      .option("spark.sql.files.maxRecordsPerFile", maxRecordsPerFile)
      .mode(SaveMode.Append)
      .parquet(outFolder)

    rows.unpersist()
  }

  private implicit class DataframeOps[A](w: DataFrameWriter[A]) {
    def withCompression(compression: Compression): DataFrameWriter[A] =
      compression match {
        case Compression.None => w
        case Compression.Gzip => w.option("compression", "gzip")
      }
  }
}
