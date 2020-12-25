package com.snowplowanalytics.snowplow.shredder.spark

import com.snowplowanalytics.snowplow.rdbloader.common.Config.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.{Config, LoaderMessage}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrameWriter}
import org.apache.spark.sql.types.{StructField, StructType, StringType}
import com.snowplowanalytics.snowplow.shredder.transformation.Shredded

object Sink {

  def writeEvents(spark: SparkSession, compression: Compression, events: RDD[Row], outFolder: String): Unit =
    spark.createDataFrame(events, StructType(StructField("_", StringType, true) :: Nil))
      .write
      .withCompression(compression)
      .mode(SaveMode.Overwrite)
      .text(getAlteredEnrichedOutputPath(outFolder))

  def writeShredded(spark: SparkSession, compression: Compression, formats: Config.Formats, shreddedData: RDD[Shredded], outFolder: String): Unit = {
    val canBeJson = formats.default == LoaderMessage.Format.JSON || formats.json.nonEmpty
    val canBeTsv = formats.default == LoaderMessage.Format.TSV || formats.tsv.nonEmpty
    if (canBeJson) writeShredded(spark, compression, shreddedData.flatMap(_.json), true, outFolder)
    if (canBeTsv) writeShredded(spark, compression, shreddedData.flatMap(_.tabular), false, outFolder)
  }

  def writeShredded(spark: SparkSession, compression: Compression, data: RDD[(String, String, String, String, String)], json: Boolean, outFolder: String): Unit = {
    import spark.implicits._
    data
      .toDF("vendor", "name", "format", "version", "data")
      .write
      .withCompression(compression)
      .partitionBy("vendor", "name", "format", "version")
      .mode(SaveMode.Append)
      .text(getShreddedTypesOutputPath(outFolder, json))
  }

  def writeBad(spark: SparkSession, compression: Compression, shreddedBad: RDD[Row], outFolder: String): Unit =
    spark.createDataFrame(shreddedBad, StructType(StructField("_", StringType, true) :: Nil))
      .write
      .withCompression(compression)
      .mode(SaveMode.Overwrite)
      .text(outFolder)


  /**
   * The path at which to store the shredded types.
   * @param outFolder shredded/good/run=xxx
   * @param json pre-R31 output path
   * @return The shredded types output path
   */
  def getShreddedTypesOutputPath(outFolder: String, json: Boolean): String = {
    val shreddedTypesSubdirectory = if (json) "shredded-types" else "shredded-tsv"
    s"$outFolder${if (outFolder.endsWith("/")) "" else "/"}$shreddedTypesSubdirectory"
  }
  /**
   * The path at which to store the altered enriched events.
   * @param outFolder shredded/good/run=xxx
   * @return The altered enriched event path
   */
  def getAlteredEnrichedOutputPath(outFolder: String): String = {
    val alteredEnrichedEventSubdirectory = "atomic-events"
    s"$outFolder${if (outFolder.endsWith("/")) "" else "/"}$alteredEnrichedEventSubdirectory"
  }

  private implicit class DataframeOps[A](w: DataFrameWriter[A]) {
    def withCompression(compression: Compression): DataFrameWriter[A] =
      compression match {
        case Compression.None => w
        case Compression.Gzip => w.option("compression", "gzip")
      }
  }
}
