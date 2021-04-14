/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.loading

import com.snowplowanalytics.snowplow.rdbloader.db.Statement

// This project
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.common.config.Config.Shredder.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.StorageTarget.Redshift
import com.snowplowanalytics.snowplow.rdbloader.common.config.{Config, Step}


/**
 * Result of discovery and SQL-statement generation steps
 *
 * @param dbSchema common DB schema (e.g. atomic)
 * @param atomicCopy COPY FROM statement to load `events` table
 * @param shredded COPY FROM statements to load shredded tables
 * @param vacuum VACUUM statements **including `events` table** if necessary
 * @param analyze ANALYZE statements **including `events` table** if necessary
 * @param discovery original discovery object
 */
case class RedshiftStatements(
    dbSchema: String,
    atomicCopy: Statement.EventsCopy,
    shredded: List[Statement.ShreddedCopy],
    vacuum: Option[List[Statement.Vacuum]],
    analyze: Option[List[Statement.Analyze]])

object RedshiftStatements {

  val EventFieldSeparator = "\t"

  /**
   * Transform discovery results into group of load statements (atomic, shredded, etc)
   * More than one `RedshiftLoadStatements` must be grouped with others using `buildQueue`
   */
  private[loading] def getStatements(config: Config[Redshift], discovery: DataDiscovery): RedshiftStatements = {
    val shreddedStatements = discovery
      .shreddedTypes
      .filterNot(_.isAtomic)
      .map(transformShreddedType(config, discovery.compression))
    val transitCopy = config.steps.contains(Step.TransitCopy)
    val atomic = Statement.EventsCopy(config.storage.schema, transitCopy, discovery.base, config.region, config.storage.maxError, config.storage.roleArn, discovery.compression)
    buildLoadStatements(config.storage, config.steps, atomic, shreddedStatements)
  }

  /**
   * Constructor for `RedshiftLoadStatements`. Deconstructs discovered
   * statements and adds only those that are required based
   * on passed `steps` argument
   *
   * @param target Redshift storage target configuration
   * @param steps SQL steps
   * @param atomicCopy a way to copy data into atomic events table
   * @param shreddedStatements statements for shredded tables (include COPY,
   *                           ANALYZE and VACUUM)
   * @return statements ready to be executed on Redshift
   */
  def buildLoadStatements(target: Redshift,
                          steps: Set[Step],
                          atomicCopy: Statement.EventsCopy,
                          shreddedStatements: List[ShreddedStatements]): RedshiftStatements = {
    val shreddedCopyStatements = shreddedStatements.map(_.copy)

    val eventsTable = EventsTable.withSchema(target)

    // Vacuum all tables including events-table
    val vacuum = if (steps.contains(Step.Vacuum)) {
      val statements = Statement.Vacuum(eventsTable) :: shreddedStatements.map(_.vacuum)
      Some(statements)
    } else None

    // Analyze all tables including events-table
    val analyze = if (steps.contains(Step.Analyze)) {
      val statements = Statement.Analyze(eventsTable) :: shreddedStatements.map(_.analyze)
      Some(statements)
    } else None

    RedshiftStatements(target.schema, atomicCopy, shreddedCopyStatements, vacuum, analyze)
  }

  /**
   * SQL statements for particular shredded type, grouped by their purpose
   *
   * @param copy main COPY FROM statement to load shredded type in its dedicate table
   * @param analyze ANALYZE SQL-statement for dedicated table
   * @param vacuum VACUUM SQL-statement for dedicate table
   */
  private case class ShreddedStatements(copy: Statement.ShreddedCopy, analyze: Statement.Analyze, vacuum: Statement.Vacuum)

  /**
   * Build group of SQL statements for particular shredded type
   *
   * @param config main Snowplow configuration
   * @param shreddedType full info about shredded type found in `shredded/good`
   * @return three SQL-statements to load `shreddedType` from S3
   */
  private def transformShreddedType(config: Config[Redshift], compression: Compression)(shreddedType: ShreddedType): ShreddedStatements = {
    val tableName = config.storage.shreddedTable(shreddedType.getTableName)
    val copyFromJson = Statement.ShreddedCopy(config.storage.schema, shreddedType, config.region, config.storage.maxError, config.storage.roleArn, compression)
    val analyze = Statement.Analyze(tableName)
    val vacuum = Statement.Vacuum(tableName)
    ShreddedStatements(copyFromJson, analyze, vacuum)
  }
}
