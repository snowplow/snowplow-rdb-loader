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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession, SaveMode, DataFrameWriter}
import org.apache.spark.sql.types.StructType
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression


object Sink {

  def writeShredded(spark: SparkSession, compression: Compression, data: RDD[(String, String, String, String, Int, String)], outFolder: String): Unit = {
    import spark.implicits._
    data
      .toDF("output", "vendor", "name", "format", "model", "data")
      .write
      .withCompression(compression)
      .partitionBy("output", "vendor", "name", "format", "model")
      .mode(SaveMode.Append)
      .text(outFolder)
  }

  def writeWideRowed(spark: SparkSession, compression: Compression, data: RDD[(String, String)], outFolder: String): Unit = {
    import spark.implicits._
    data
      .toDF("output", "data")
      .write
      .withCompression(compression)
      .partitionBy("output")
      .mode(SaveMode.Append)
      .text(outFolder)
  }

  def writeParquet(spark: SparkSession, sparkSchema: StructType, data: RDD[List[Any]], outFolder: String): Unit = {
    val rows = data.map(Row.fromSeq)
    spark.createDataFrame(rows, sparkSchema)
      .write
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
