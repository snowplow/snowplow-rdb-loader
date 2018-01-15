/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader
package loaders

import shapeless.tag
import shapeless.tag._

// This project
import discovery.DataDiscovery
import config.{ CliConfig, Step }
import config.StorageTarget.{ PostgresqlConfig, RedshiftConfig }


object Common {

  /**
   * Main "atomic" table name
   */
  val EventsTable = "events"

  /**
   * Table for load manifests
   */
  val ManifestTable = "manifest"

  /**
   * Correctly merge database schema and table name
   */
  def getEventsTable(databaseSchema: String): String =
    if (databaseSchema.isEmpty) EventsTable
    else databaseSchema + "." + EventsTable

  /**
   * Correctly merge database schema and table name
   */
  def getManifestTable(databaseSchema: String): String =
    if (databaseSchema.isEmpty) ManifestTable
    else databaseSchema + "." + ManifestTable

  /**
   * Subpath to check `atomic-events` directory presence
   */
  val atomicSubpathPattern = "(.*)/(run=[0-9]{4}-[0-1][0-9]-[0-3][0-9]-[0-2][0-9]-[0-6][0-9]-[0-6][0-9]/atomic-events)/(.*)".r
  //                                    year     month      day        hour       minute     second

  /**
   * Process any valid storage target,
   * including discovering step and establishing SSH-tunnel
   *
   * @param cliConfig RDB Loader app configuration
   */
  def load(cliConfig: CliConfig, discovery: List[DataDiscovery]): LoaderAction[Unit] = {
    val loadDb = cliConfig.target match {
      case postgresqlTarget: PostgresqlConfig =>
        PostgresqlLoader.run(postgresqlTarget, cliConfig.steps, discovery)
      case redshiftTarget: RedshiftConfig =>
        RedshiftLoader.run(cliConfig.configYaml, redshiftTarget, cliConfig.steps, discovery)
    }

    Security.bracket(cliConfig.target.sshTunnel, loadDb)
  }

  /**
    * Choose a discovery strategy and perform it
    *
    * @param cliConfig RDB Loader app configuration
    */
  def discover(cliConfig: CliConfig): LoaderAction[List[DataDiscovery]] = {
    // Shortcuts
    val shredJob = cliConfig.configYaml.storage.versions.rdbShredder
    val region = cliConfig.configYaml.aws.s3.region
    val assets = cliConfig.configYaml.aws.s3.buckets.jsonpathAssets

    val target = (cliConfig.target.processingManifest, cliConfig.folder) match {
      case (None, Some(f)) =>
        DataDiscovery.InSpecificFolder(f)
      case (Some(_), f) =>
        DataDiscovery.ViaManifest(f)
      case (None, None) =>
        DataDiscovery.Global(cliConfig.configYaml.aws.s3.buckets.shredded.good)
    }

    cliConfig.target match {
      case _: RedshiftConfig =>
        val original = DataDiscovery.discoverFull(target, cliConfig.target.id, shredJob, region, assets)
        if (cliConfig.steps.contains(Step.ConsistencyCheck))
          DataDiscovery.checkConsistency(original)
        else original
      case _: PostgresqlConfig =>
        // Safe to skip consistency check as whole folder will be downloaded
        DataDiscovery.discoverAtomic(target, cliConfig.target.id)
    }
  }

  /**
   * String representing valid SQL query/statement,
   * ready to be executed
   */
  type SqlString = String @@ SqlStringTag

  object SqlString extends tag.Tagger[SqlStringTag] {
    def unsafeCoerce(s: String) = apply(s)
  }

  sealed trait SqlStringTag
}
