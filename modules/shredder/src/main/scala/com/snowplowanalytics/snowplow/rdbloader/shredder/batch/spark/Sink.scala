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
import org.apache.spark.sql.{Row, SparkSession, SaveMode, DataFrameWriter}
import org.apache.spark.sql.types.StructType

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.snowplow.rdbloader.common.Common
import com.snowplowanalytics.snowplow.rdbloader.common.config.Config.Shredder.Compression

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

    data.unpersist()
  }

  def writeParquet(spark: SparkSession, schemasMap: Map[SchemaKey, StructType], data: RDD[(String, String, String, String, Int, List[Any])], outFolder: String): Unit = {
    schemasMap.foreach {
      case (k @ SchemaKey(vendor, name, _, SchemaVer.Full(model, _, _)), sparkSchema) if k == Common.AtomicSchema =>
        val fullPath = parquetPath(vendor, name, model, outFolder)
        val filtered = data.flatMap {
          case (_, v, n, _, m, data) if v == Common.AtomicSchema.vendor && n == Common.AtomicSchema.name && m == Common.AtomicSchema.version.model =>
            Some(Row.fromSeq(data))
          case _ =>
            None
        }
        spark.createDataFrame(filtered, sparkSchema)
          .write
          .mode(SaveMode.Append)
          .parquet(fullPath)
        filtered.unpersist()
      case (SchemaKey(vendor, name, _, SchemaVer.Full(model, _, _)), sparkSchema) =>
        val fullPath = parquetPath(vendor, name, model, outFolder)
        val filtered = data.flatMap {
          case (_, v, n, _, m, data) if v == vendor && n == name && m == model =>
            Some(Row.fromSeq(data))
          case _ =>
            None
        }
        spark.createDataFrame(filtered, sparkSchema)
          .write
          .mode(SaveMode.Append)
          .parquet(fullPath)
        filtered.unpersist()
    }
  }  

  def parquetPath(vendor: String, name: String, model: Int, folder: String): String = {
    val withTrailing = if (folder.endsWith("/")) folder else s"$folder/"
    withTrailing ++ s"/output=good/vendor=$vendor/name=$name/format=parquet/model=$model"
  }

  private implicit class DataframeOps[A](w: DataFrameWriter[A]) {
    def withCompression(compression: Compression): DataFrameWriter[A] =
      compression match {
        case Compression.None => w
        case Compression.Gzip => w.option("compression", "gzip")
      }
  }
}
