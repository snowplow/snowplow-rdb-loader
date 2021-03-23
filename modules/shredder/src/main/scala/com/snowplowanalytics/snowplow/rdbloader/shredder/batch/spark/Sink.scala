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
package com.snowplowanalytics.snowplow.rdbloader.shredder.batch.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrameWriter}
import org.apache.spark.sql.types.{StructField, StructType, StringType}

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.config.Config
import com.snowplowanalytics.snowplow.rdbloader.common.config.Config.Shredder.Compression

import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Shredded

object Sink {

  def writeShredded(spark: SparkSession, compression: Compression, formats: Config.Formats, shreddedData: RDD[Shredded], outFolder: String): Unit = {
    writeShredded(spark, compression, shreddedData.flatMap(_.tsv), outFolder)
    val canBeJson = formats.default == LoaderMessage.Format.JSON || formats.json.nonEmpty
    if (canBeJson) writeShredded(spark, compression, shreddedData.flatMap(_.json), outFolder)
  }

  def writeShredded(spark: SparkSession, compression: Compression, data: RDD[(String, String, String, Int, String)], outFolder: String): Unit = {
    import spark.implicits._
    data
      .toDF("vendor", "name", "format", "model", "data")
      .write
      .withCompression(compression)
      .partitionBy("vendor", "name", "format", "model")
      .mode(SaveMode.Append)
      .text(outFolder)
  }

  def writeBad(spark: SparkSession, compression: Compression, shreddedBad: RDD[Row], outFolder: String): Unit =
    spark.createDataFrame(shreddedBad, StructType(StructField("_", StringType, true) :: Nil))
      .write
      .withCompression(compression)
      .mode(SaveMode.Overwrite)
      .text(outFolder)

  private implicit class DataframeOps[A](w: DataFrameWriter[A]) {
    def withCompression(compression: Compression): DataFrameWriter[A] =
      compression match {
        case Compression.None => w
        case Compression.Gzip => w.option("compression", "gzip")
      }
  }
}
