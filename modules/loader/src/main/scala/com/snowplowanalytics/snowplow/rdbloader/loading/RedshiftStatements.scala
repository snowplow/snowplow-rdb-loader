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
import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget.Redshift
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Compression

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
case class RedshiftStatements(dbSchema: String, atomicCopy: Statement.EventsCopy, shredded: List[Statement.ShreddedCopy])

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
    val atomic = Statement.EventsCopy(config.storage.schema, false, discovery.base, config.region.name, config.storage.maxError, config.storage.roleArn, discovery.compression)
    buildLoadStatements(config.storage, atomic, shreddedStatements)
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
                          atomicCopy: Statement.EventsCopy,
                          shreddedStatements: List[ShreddedStatements]): RedshiftStatements = {
    val shreddedCopyStatements = shreddedStatements.map(_.copy)
    RedshiftStatements(target.schema, atomicCopy, shreddedCopyStatements)
  }

  /**
   * SQL statements for particular shredded type, grouped by their purpose
   *
   * @param copy main COPY FROM statement to load shredded type in its dedicate table
   * @param analyze ANALYZE SQL-statement for dedicated table
   * @param vacuum VACUUM SQL-statement for dedicate table
   */
  private case class ShreddedStatements(copy: Statement.ShreddedCopy)

  /**
   * Build group of SQL statements for particular shredded type
   *
   * @param config main Snowplow configuration
   * @param shreddedType full info about shredded type found in `shredded/good`
   * @return three SQL-statements to load `shreddedType` from S3
   */
  private def transformShreddedType(config: Config[Redshift], compression: Compression)(shreddedType: ShreddedType): ShreddedStatements = {
    val copyFromJson = Statement.ShreddedCopy(config.storage.schema, shreddedType, config.region.name, config.storage.maxError, config.storage.roleArn, compression)
    ShreddedStatements(copyFromJson)
  }
}
