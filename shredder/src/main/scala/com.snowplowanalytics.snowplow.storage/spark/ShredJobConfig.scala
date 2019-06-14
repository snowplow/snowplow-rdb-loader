/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics
package snowplow
package storage.spark

import java.nio.charset.StandardCharsets.UTF_8

import io.circe.Json
import io.circe.parser.parse

import cats.data.ValidatedNel
import cats.implicits._

import com.monovore.decline.{Command, Opts}

import org.apache.commons.codec.DecoderException
import org.apache.commons.codec.binary.Base64

import com.snowplowanalytics.snowplow.rdbloader.generated.ProjectMetadata
import com.snowplowanalytics.snowplow.rdbloader.common._

/**
 * Case class representing the configuration for the shred job.
 * @param inFolder Folder where the input events are located
 * @param outFolder Output folder where the shredded events will be stored
 * @param badFolder Output folder where the malformed events will be stored
 * @param igluConfig JSON representing the Iglu configuration
 */
case class ShredJobConfig(inFolder: String,
                          outFolder: String,
                          badFolder: String,
                          igluConfig: Json,
                          duplicateStorageConfig: Option[Json],
                          dynamodbManifestTable: Option[String],
                          itemId: Option[String],
                          storage: Option[StorageTarget]) {

  /** Get both manifest table and item id to process */
  def getManifestData: Option[(String, String)] =
    for {
      t <- dynamodbManifestTable
      i <- itemId
    } yield (t, i)
}

object ShredJobConfig {

  object Base64Json {
    val decoder = new Base64(true)

    def decode(str: String): ValidatedNel[String, Json] = {
      Either
        .catchOnly[DecoderException](decoder.decode(str))
        .map(arr => new String(arr, UTF_8))
        .leftMap(_.getMessage)
        .flatMap(str => parse(str).leftMap(_.show))
        .toValidatedNel
    }
  }

  val inputFolder = Opts.option[String]("input-folder",
    "Folder where the input events are located",
    metavar = "<path>")
  val outputFolder = Opts.option[String]("output-folder",
    "Output folder where the shredded events will be stored",
    metavar = "<path>")
  val badFolder = Opts.option[String]("bad-folder",
    "Output folder where the malformed events will be stored",
    metavar = "<path>")
  val igluConfig = Opts.option[String]("iglu-config",
    "Base64-encoded Iglu Client JSON config",
    metavar = "<base64>").mapValidated(Base64Json.decode)

  val duplicateStorageConfig = Opts.option[String]("duplicate-storage-config",
    "Base64-encoded Events Manifest JSON config",
    metavar = "<base64>").mapValidated(Base64Json.decode).orNone

  val processingManifestTable = Opts.option[String]("processing-manifest-table",
    "Processing manifest table",
    metavar = "<name>").orNone
  val itemId = Opts.option[String]("item-id",
    "Unique folder identificator for processing manifest (e.g. S3 URL)",
    metavar = "<id>").orNone
  val storageTarget = Opts.option[String]("target",
    "base64-encoded string with single storage target configuration JSON", "t", "target.json")
    .mapValidated(Base64Json.decode).orNone

  val shredJobConfig = (inputFolder, outputFolder, badFolder, igluConfig, duplicateStorageConfig, processingManifestTable, itemId, storageTarget).mapN {
    (input, output, bad, iglu, dupeStorage, manifest, itemId, target) => (ShredJobConfig(input, output, bad, iglu, dupeStorage, manifest, itemId, None), target)
  }.validate("--item-id and --processing-manifest-table must be either both provided or both absent") {
    case (ShredJobConfig(_, _, _, _, _, manifest, i, _), _) => (manifest.isDefined && i.isDefined) || (manifest.isEmpty && i.isEmpty)
    case _ => false
  }.mapValidated {
    case (config, Some(target)) =>
      val client = singleton.IgluSingleton.get(config.igluConfig)
      StorageTarget.parseTarget(client, target.noSpaces)
        .leftMap(_.message)
        .map(storage => config.copy(storage = Some(storage)))
        .toValidatedNel
    case (config, None) => config.validNel
  }

  val command = Command(s"${ProjectMetadata.shredderName}-${ProjectMetadata.shredderVersion}",
    "Apache Spark job to prepare Snowplow enriched data to being loaded into Amazon Redshift warehouse")(shredJobConfig)

  /**
   * Load a ShredJobConfig from command line arguments.
   * @param args The command line arguments
   * @return The job config or one or more error messages boxed in a Scalaz Validation Nel
   */
  def loadConfigFrom(args: Array[String]): Either[String, ShredJobConfig] =
    command.parse(args).leftMap(_.toString())
}
