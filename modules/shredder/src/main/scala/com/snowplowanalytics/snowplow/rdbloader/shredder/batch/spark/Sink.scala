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
import org.apache.spark.sql.{SparkSession, SaveMode, DataFrameWriter}

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.ShreddedType
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
  }

  // The problem
  // We have RDD. And this RDD represents the whole heterogenius dataset
  // But when we write to Parquet - we need to know the schema upfront

  def writeParquet(spark: SparkSession, compression: Compression, data: RDD[(String, String, String, String, Int, String)], outFolder: String, types: List[ShreddedType]): Unit = {
    println(spark)
    println(compression)
    println(data)
    println(outFolder)
    types.map(_.schemaKey).foreach {
     case SchemaKey(vendor, name, _, SchemaVer.Full(model, _, _)) =>
       val ordered = Flattening.getOrdered(IgluSingleton.get(shredConfig.igluConfig).resolver, vendor, name, model).value.right.get
       val sparkSchema = Columnar.getSparkSchema(ordered)
       ???
   }



    ???
  }

  private implicit class DataframeOps[A](w: DataFrameWriter[A]) {
    def withCompression(compression: Compression): DataFrameWriter[A] =
      compression match {
        case Compression.None => w
        case Compression.Gzip => w.option("compression", "gzip")
      }
  }
}
