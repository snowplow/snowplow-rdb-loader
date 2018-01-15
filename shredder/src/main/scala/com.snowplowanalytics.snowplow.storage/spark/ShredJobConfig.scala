/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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

// Scalaz
import scalaz._
import Scalaz._

// Snowplow
import iglu.client.validation.ProcessingMessageMethods._
import enrich.common.ValidatedNelMessage

/**
 * Case class representing the configuration for the shred job.
 * @param inFolder Folder where the input events are located
 * @param outFolder Output folder where the shredded events will be stored
 * @param badFolder Output folder where the malformed events will be stored
 * @param igluConfig JSON representing the Iglu configuration
 */
case class ShredJobConfig(
  inFolder: String = "",
  outFolder: String = "",
  badFolder: String = "",
  igluConfig: String = "",
  duplicateStorageConfig: Option[String] = None,
  dynamodbManifestTable: Option[String] = None,
  itemId: Option[String] = None
) {
  /** Get both manifest table and item id to process */
  def getManifestData: Option[(String, String)] =
    for {
      t <- dynamodbManifestTable
      i <- itemId
    } yield (t, i)
}

object ShredJobConfig {
  private val parser = new scopt.OptionParser[ShredJobConfig]("ShredJob") {
    head("ShredJob")
    opt[String]("input-folder").required().valueName("<input folder>")
      .action((f, c) => c.copy(inFolder = f))
      .text("Folder where the input events are located")
    opt[String]("output-folder").required().valueName("<output folder>")
      .action((f, c) => c.copy(outFolder = f))
      .text("Output folder where the shredded events will be stored")
    opt[String]("bad-folder").required().valueName("<bad folder>")
      .action((f, c) => c.copy(badFolder = f))
      .text("Output folder where the malformed events will be stored")
    opt[String]("iglu-config").required().valueName("<iglu config>")
      .action((i, c) => c.copy(igluConfig = i))
      .text("Iglu configuration")
    opt[String]("duplicate-storage-config").optional().valueName("<duplicate storage config")
      .action((d, c) => c.copy(duplicateStorageConfig = Some(d)))
      .text("Duplicate storage configuration")
    opt[String]("processing-manifest-table").optional().valueName("<dynamodb table name>")
      .action((d, c) => c.copy(dynamodbManifestTable = Some(d)))
      .text("Processing manifest table")
    opt[String]("item-id").optional().valueName("<id>")
      .action((d, c) => c.copy(itemId = Some(d)))
      .text("Unique folder identificator for processing manifest (e.g. S3 URL)")
    help("help").text("Prints this usage text")
    checkConfig(c =>
      (c.dynamodbManifestTable, c.itemId) match {
        case (Some(_), Some(_)) => success
        case (None, None) => success
        case (t, i) => failure(s"Both --processing-manifest-table and --item-id-should be either provided or omitted. Processing manifest: $t, item id: $i")
      }
    )
  }

  /**
   * Load a ShredJobConfig from command line arguments.
   * @param args The command line arguments
   * @return The job config or one or more error messages boxed in a Scalaz Validation Nel
   */
  def loadConfigFrom(args: Array[String]): ValidatedNelMessage[ShredJobConfig] =
    parser.parse(args, ShredJobConfig()) match {
      // We try to build the resolver early to detect failures before starting the job
      case Some(c) => singleton.ResolverSingleton.getIgluResolver(c.igluConfig).map(_ => c)
      case None => "Parsing of the configuration failed".toProcessingMessage.failureNel
    }
}
