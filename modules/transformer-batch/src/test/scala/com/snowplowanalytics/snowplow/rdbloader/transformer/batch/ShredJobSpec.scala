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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import cats.Id
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowplow.rdbloader.common.catsClockIdInstance
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.{AtomicFieldsProvider, NonAtomicFieldsProvider}
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields.AllFields
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark.singleton.IgluSingleton

import java.io.{BufferedWriter, File, FileWriter, IOException}
import java.util.Base64
import java.net.URI
import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.Random
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

// Commons
import org.apache.commons.io.filefilter.IOFileFilter
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter

import cats.data.ValidatedNel
import cats.implicits._

import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import io.circe.optics.JsonPath._
import io.circe.parser.{parse => parseCirce}

// Json4s
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse}

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.iglu.core.SchemaCriterion

import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifestConfig

import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.config.{Region, TransformerConfig}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Formats.WideRow

// Specs2
import org.specs2.matcher.Matcher
import org.specs2.matcher.Matchers._

object ShredJobSpec {

  val Name = "snowplow-transformer-batch"
  val Version = BuildInfo.version

  val AtomicFolder = "vendor=com.snowplowanalytics.snowplow/name=atomic/format=tsv/model=1"

  sealed trait Events

  case class ResourceFile(resourcePath: String) extends Events {
    def toUri: URI = getClass.getResource(resourcePath).toURI
  }

  /** Case class representing the input lines written in a file. */
  case class Lines(l: String*) extends Events {
    val lines = l.toList

    /** Write the lines to a file. */
    def writeTo(file: File): Unit = {
      val writer = new BufferedWriter(new FileWriter(file))
      for (line <- lines) {
        writer.write(line)
        writer.newLine()
      }
      writer.close()
    }

    def apply(i: Int): String = lines(i)
  }

  /** Case class representing the directories where the output of the job has been written. */
  case class OutputDirs(output: File) {
    val goodRows: File = new File(output, "output=good")
    val badRows: File = new File(output, "output=bad")
  }

  def read(f: String): List[String] = {
    val source = Source.fromFile(new File(f))
    val lines = source.getLines.toList
    source.close()
    lines
  }

  /**
   * Read a part file at the given path into a List of Strings
   * @param root
   *   A root filepath
   * @param relativePath
   *   The relative path to the file from the root
   * @return
   *   the file contents as well as the file name
   */
  def readPartFile(root: File, relativePath: String = "/"): Option[(List[String], String)] = {
    val files = listFilesWithExclusions(new File(root, relativePath), List.empty)
      .filter(s => s.contains("part-"))
    files.foldLeft[Option[(List[String], String)]](None) { (acc, f) =>
      val accValue = acc.getOrElse((List.empty, ""))
      val contents = accValue._1 ++ read(f)
      Some((contents, f))
    }
  }

  def readParquetFile(spark: SparkSession, root: File): List[Json] =
    spark.read
      .parquet(root.toString)
      .toJSON
      .collectAsList()
      .asScala
      .toList
      .flatMap(parseCirce(_).toOption)

  def readParquetFields(spark: SparkSession, root: File): List[StructField] =
    spark.read.parquet(root.toString).schema.fields.toList

  def readResourceFile(resourceFile: ResourceFile): List[String] =
    read(resourceFile.toUri.getPath)

  /**
   * Ignore empty files on output (necessary since
   * https://github.com/snowplow/snowplow-rdb-loader/issues/142)
   */
  val NonEmpty = new IOFileFilter {
    def accept(file: File): Boolean = file.length() > 1L
    def accept(dir: File, name: String): Boolean = true
  }

  /**
   * Recursively list files in a given path, excluding the supplied paths.
   * @param root
   *   A root filepath
   * @param exclusions
   *   A list of paths to exclude from the listing
   * @return
   *   the list of files contained in the root, minus the exclusions
   */
  def listFilesWithExclusions(root: File, exclusions: List[String]): List[String] =
    try
      FileUtils
        .listFiles(root, NonEmpty, TrueFileFilter.TRUE)
        .asScala
        .toList
        .map(_.getCanonicalPath)
        .filter(p => !exclusions.contains(p) && !p.contains("crc") && !p.contains("SUCCESS"))
    catch {
      case e: IllegalArgumentException if e.getMessage.contains("Parameter 'directory' is not a directory") =>
        Nil
    }

  /** A Specs2 matcher to check if a directory on disk is empty or not. */
  val beEmptyDir: Matcher[File] =
    (
      (f: File) =>
        !f.isDirectory ||
          f.list().length == 0 ||
          f.listFiles().filter(f => f.getName != "_SUCCESS" && !f.getName.endsWith(".crc")).map(_.length).sum == 0,
      "is populated dir"
    )

  /**
   * Delete a file or directory and its contents recursively. Throws an exception if deletion is
   * unsuccessful.
   */
  def deleteRecursively(file: File): Unit = {
    def listFilesSafely(file: File): Seq[File] =
      if (file.exists()) {
        val files = file.listFiles()
        if (files == null) throw new IOException(s"Failed to list files for dir: $file")
        files
      } else {
        Seq.empty[File]
      }

    try
      if (file.isDirectory) {
        var savedIOException: IOException = null
        for (child <- listFilesSafely(file))
          try
            deleteRecursively(child)
          catch {
            // In case of multiple exceptions, only last one will be thrown
            case ioe: IOException => savedIOException = ioe
          }
        if (savedIOException != null) throw savedIOException
      }
    finally
      if (!file.delete()) {
        // Delete can also fail if the file simply did not exist
        if (file.exists()) throw new IOException(s"Failed to delete: ${file.getAbsolutePath}")
      }
  }

  /**
   * Make a temporary file optionally filling it with contents.
   * @param tag
   *   an identifier who will become part of the file name
   * @param createParents
   *   whether or not to create the parent directories
   * @param containing
   *   the optional contents
   * @return
   *   the created file
   */
  def mkTmpFile(
    tag: String,
    createParents: Boolean = false,
    containing: Option[Lines] = None
  ): File = {
    val f = File.createTempFile(s"snowplow-shred-job-$tag-", "")
    if (createParents) f.mkdirs() else f.mkdir()
    containing.foreach(_.writeTo(f))
    f
  }

  /**
   * Create a file with the specified name with a random number at the end.
   * @param tag
   *   an identifier who will become part of the file name
   * @return
   *   the created file
   */
  def randomFile(tag: String): File =
    new File(System.getProperty("java.io.tmpdir"), s"snowplow-shred-job-${tag}-${Random.nextInt(Int.MaxValue)}")

  /** Remove the timestamp from bad rows so that what remains is deterministic */
  def removeTstamp(badRow: String): String = {
    val badRowJson = parse(badRow)
    val badRowWithoutTimestamp =
      ("line", badRowJson \ "line") ~ (("errors", badRowJson \ "errors"))
    compact(badRowWithoutTimestamp)
  }

  def setTimestamp(timestamp: String)(s: String): String = {
    val setter = root.data.failure.each.error.lookupHistory.each.lastAttempt.string.set(timestamp)
    parseCirce(s).map(setter).map(_.noSpaces).getOrElse(s)
  }

  private def storageConfig(
    shredder: Config,
    tsv: Boolean,
    jsonSchemas: List[SchemaCriterion],
    wideRow: Option[WideRow]
  ) = {
    val encoder = Base64.getUrlEncoder
    val format = if (tsv) "TSV" else "JSON"
    val jsonCriterions = jsonSchemas.map(x => s""""${x.asString}"""").mkString(",")
    val formatsSection = wideRow match {
      case Some(WideRow.PARQUET) =>
        s"""
           |"formats": {
           |  "transformationType": "widerow"
           |  "fileFormat": "parquet"
           |}
           |""".stripMargin
      case Some(WideRow.JSON) =>
        s"""
           |"formats": {
           |  "transformationType": "widerow"
           |  "fileFormat": "json"
           |}
           |""".stripMargin
      case None =>
        s"""
           | "formats": {
           |   "transformationType": "shred"
           |   "default": "$format",
           |   "json": [$jsonCriterions],
           |   "tsv": [ ],
           |   "skip": [ ]
           | }
           |""".stripMargin
    }
    val syntheticDeduplication = shredder.deduplication.synthetic match {
      case Config.Deduplication.Synthetic.None => """{"type": "none"}"""
      case Config.Deduplication.Synthetic.Join => """{"type": "join"}"""
      case Config.Deduplication.Synthetic.Broadcast(cardinality) => s"""{"type": "broadcast", "cardinality": $cardinality}"""
    }
    val naturalDeduplication = shredder.deduplication.natural.asJson
    val configPlain = s"""|{
    |"input": "${shredder.input}",
    |"output" = {
    |  "path": "${shredder.output.path}",
    |  "compression": "${shredder.output.compression.toString.toUpperCase}",
    |  "region": "us-east-1"
    |},
    |"queue": {
    |  "type": "SQS"
    |  "queueName": "test-sqs"
    |  "region": "us-east-1"
    |}
    |$formatsSection
    |"deduplication": {
    |  "synthetic": $syntheticDeduplication
    |  "natural": $naturalDeduplication
    |}
    |"validations": {
    |    "minimumTimestamp": "0000-01-02T00:00:00.00Z"
    |}
    |"monitoring": {"snowplow": null, "sentry": null}
    |}""".stripMargin
    new String(encoder.encode(configPlain.getBytes()))
  }

  private val igluConfigWithLocal = {
    val encoder = Base64.getUrlEncoder
    new String(
      encoder.encode(
        """|{
         |"schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
         |"data": {
         |"cacheSize": 500,
         |"repositories": [
         |{
         |"name": "Local Iglu Server",
         |"priority": 0,
         |"vendorPrefixes": [ "com.snowplowanalytics" ],
         |"connection": {
         |"http": {
         |"uri": "http://localhost:8080/api"
         |}
         |}
         |},
         |{
         |"name": "Iglu Central",
         |"priority": 0,
         |"vendorPrefixes": [ "com.snowplowanalytics" ],
         |"connection": {
         |"http": {
         |"uri": "http://iglucentral.com"
         |}
         |}
         |},
         |{
         |"name": "Iglu Central Mirror",
         |"priority": 0,
         |"vendorPrefixes": [ "com.snowplowanalytics" ],
         |"connection": {
         |"http": {
         |"uri": "https://com-iglucentral-eu1-prod.iglu.snplow.net/api"
         |}
         |}
         |},
         |{
         |"name": "Iglu Embedded",
         |"priority": 0,
         |"vendorPrefixes": [ "com.snowplowanalytics" ],
         |"connection": {
         |"embedded": {
         |"path": "/widerow/parquet"
         |}
         |}
         |}
         |]
         |}
         |}""".stripMargin.replaceAll("[\n\r]", "").getBytes()
      )
    )

  }

  val dynamodbDuplicateStorageTable = "snowplow-integration-test-crossbatch-deduplication"
  val dynamodbDuplicateStorageRegion = "us-east-1"

  /**
   * Duplicate storage configuration, enabling cross-batch deduplication on CI environment If CI is
   * set and all envvars are available it becomes valid schema If not all envvars are available, but
   * CI is set - it will throw runtime exception as impoperly configured CI environment If not all
   * envvars are available, but CI isn't set - it will return empty JSON, which should not be used
   * anywhere (in JobSpecHelpers)
   */
  val duplicateStorageConfig =
    json"""{
      "schema": "iglu:com.snowplowanalytics.snowplow.storage/amazon_dynamodb_config/jsonschema/2-0-0",
      "data": {
        "name": "local",
        "auth": null,
        "awsRegion": $dynamodbDuplicateStorageRegion,
        "dynamodbTable": $dynamodbDuplicateStorageTable,
        "purpose": "EVENTS_MANIFEST"
      }
    }"""

  val DefaultTimestamp = "2020-09-29T10:38:56.653Z"

  val clearTimestamp: Json => Json =
    root.data.failure.timestamp.string.set(DefaultTimestamp)

  def clearFailureTimestamps(jsons: List[String]): List[String] =
    jsons
      .map(parseCirce)
      .sequence
      .map(_.map(clearTimestamp))
      .toOption
      .get
      .map(_.noSpaces)

  /** Get environment variable wrapped into `Validation` */
  def getEnv(envvar: String): ValidatedNel[String, String] = sys.env.get(envvar) match {
    case Some(v) => v.validNel
    case None => s"Environment variable [$envvar] is not available".invalidNel
  }

  def getShredder(
    events: Events,
    dirs: OutputDirs,
    deduplication: Config.Deduplication
  ): Config = {
    val input = events match {
      case r: ResourceFile => r.toUri
      case l: Lines => mkTmpFile("input", createParents = true, containing = Some(l)).toURI
    }
    Config(
      input,
      Config.Output(
        dirs.output.toURI,
        TransformerConfig.Compression.None,
        Region("eu-central-1"),
        maxRecordsPerFile = 10000,
        Config.Output.BadSink.File
      ),
      Config.QueueConfig.SQS("test-sqs", Region("eu-central-1")),
      TransformerConfig.Formats.Shred(LoaderMessage.TypesInfo.Shredded.ShreddedFormat.TSV, Nil, Nil, Nil),
      Config.Monitoring(None),
      deduplication,
      Config.RunInterval(None, None, None),
      TransformerConfig.FeatureFlags(false, None, false),
      TransformerConfig.Validations(None)
    )
  }
}

/** Trait to mix in in every spec for the shred job. */
trait ShredJobSpec extends SparkSpec {
  import ShredJobSpec._

  val dirs = OutputDirs(randomFile("output"))

  val VersionPlaceholder = "version_placeholder"

  /**
   * Run the shred job with the specified lines as input.
   * @param lines
   *   input lines
   */
  def runShredJob(
    events: Events,
    crossBatchDedupe: Boolean = false,
    tsv: Boolean = false,
    jsonSchemas: List[SchemaCriterion] = Nil,
    wideRow: Option[WideRow] = None,
    outputDirs: Option[OutputDirs] = None,
    deduplication: Config.Deduplication = Config.Deduplication(Config.Deduplication.Synthetic.Broadcast(1), true)
  ): LoaderMessage.ShreddingComplete = {
    val shredder = getShredder(events, outputDirs.getOrElse(dirs), deduplication)
    val config = Array(
      "--iglu-config",
      igluConfigWithLocal,
      "--config",
      storageConfig(shredder, tsv, jsonSchemas, wideRow)
    )

    val (dedupeConfigCli, dedupeConfig) = if (crossBatchDedupe) {
      val encoder = Base64.getUrlEncoder
      val encoded = new String(encoder.encode(duplicateStorageConfig.noSpaces.getBytes()))
      val config = SelfDescribingData
        .parse(duplicateStorageConfig)
        .leftMap(_.code)
        .flatMap(EventsManifestConfig.DynamoDb.extract)
        .valueOr(e => throw new RuntimeException(e))
      (Array("--duplicate-storage-config", encoded), Some(config))
    } else {
      (Array.empty[String], None)
    }

    CliConfig.loadConfigFrom("snowplow-rdb-shredder", "Test specification for RDB Shrederr")(config ++ dedupeConfigCli) match {
      case Right(cli) =>
        val resolverConfig = Resolver
          .parseConfig(cli.igluConfig)
          .valueOr(error => throw new IllegalArgumentException(s"Could not parse iglu resolver config: ${error.getMessage()}"))

        val transformer = cli.config.formats match {
          case f: TransformerConfig.Formats.Shred => Transformer.ShredTransformer(resolverConfig, f, Map.empty, 0)
          case TransformerConfig.Formats.WideRow.JSON => Transformer.WideRowJsonTransformer()
          case TransformerConfig.Formats.WideRow.PARQUET =>
            val resolver = IgluSingleton.get(resolverConfig)
            val allTypesForRun = (new TypeAccumJob(spark, cli.config)).run("")

            val nonAtomicFields = NonAtomicFieldsProvider.build[Id](resolver, allTypesForRun).value.right.get
            val allFields = AllFields(AtomicFieldsProvider.static, nonAtomicFields)
            val schema = SparkSchema.build(allFields)

            Transformer.WideRowParquetTransformer(allFields, schema)
        }
        val job = new ShredJob(spark, transformer, cli.config)
        val result = job.run("", dedupeConfig)
        deleteRecursively(new File(cli.config.input))
        result
      case Left(e) =>
        throw new RuntimeException(s"Cannot parse test configuration. Error: $e")
    }
  }

  override def afterAll(): Unit =
    super.afterAll()
}
