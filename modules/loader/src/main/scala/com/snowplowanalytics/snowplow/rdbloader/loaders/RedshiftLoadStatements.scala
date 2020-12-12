/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.loaders

// This project
import com.snowplowanalytics.snowplow.rdbloader.common.{S3, Config, Step}
import com.snowplowanalytics.snowplow.rdbloader.common.StorageTarget.Redshift
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.loaders.RedshiftLoadStatements._
import com.snowplowanalytics.snowplow.rdbloader.loaders.Common.{TransitTable, AtomicEvents, SqlString}


/**
 * Result of discovery and SQL-statement generation steps
 *
 * @param dbSchema common DB schema (e.g. atomic)
 * @param atomicCopy COPY FROM statement to load `events` table
 * @param shredded COPY FROM statements to load shredded tables
 * @param vacuum VACUUM statements **including `events` table** if necessary
 * @param analyze ANALYZE statements **including `events` table** if necessary
 * @param manifest SQL statement to populate `manifest` table
 * @param discovery original discovery object
 */
case class RedshiftLoadStatements(
    dbSchema: String,
    atomicCopy: AtomicCopy,
    shredded: List[SqlString],
    vacuum: Option[List[SqlString]],
    analyze: Option[List[SqlString]],
    discovery: DataDiscovery) {
  def base = discovery.base
}

object RedshiftLoadStatements {

  val EventFieldSeparator = "\t"

  sealed trait AtomicCopy
  object AtomicCopy {
    final case class Straight(copy: SqlString) extends AtomicCopy
    final case class Transit(copy: SqlString) extends AtomicCopy
  }

  /**
   * Transform discovery results into group of load statements (atomic, shredded, etc)
   * More than one `RedshiftLoadStatements` must be grouped with others using `buildQueue`
   */
  private[loaders] def getStatements(config: Config[Redshift], discovery: DataDiscovery): RedshiftLoadStatements = {
    val shreddedStatements = discovery.shreddedTypes.map(transformShreddedType(config, _))
    val transitCopy = config.steps.contains(Step.TransitCopy) // TODO: test with and without messageQueue
    val atomic = buildEventsCopy(config, discovery.atomicEvents, transitCopy)
    buildLoadStatements(config.storage, config.steps, atomic, shreddedStatements, discovery)
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
   * @param discovery original discovery object
   * @return statements ready to be executed on Redshift
   */
  def buildLoadStatements(target: Redshift,
                          steps: Set[Step],
                          atomicCopy: AtomicCopy,
                          shreddedStatements: List[ShreddedStatements],
                          discovery: DataDiscovery
   ): RedshiftLoadStatements = {
    val shreddedCopyStatements = shreddedStatements.map(_.copy)

    val eventsTable = Common.getEventsTable(target)

    // Vacuum all tables including events-table
    val vacuum = if (steps.contains(Step.Vacuum)) {
      val statements = buildVacuumStatement(eventsTable) :: shreddedStatements.map(_.vacuum)
      Some(statements)
    } else None

    // Analyze all tables including events-table
    val analyze = if (steps.contains(Step.Analyze)) {
      val statements = buildAnalyzeStatement(eventsTable) :: shreddedStatements.map(_.analyze)
      Some(statements)
    } else None

    RedshiftLoadStatements(target.schema, atomicCopy, shreddedCopyStatements, vacuum, analyze, discovery)
  }


  /**
   * Build COPY FROM TSV SQL-statement for atomic.events table
   *
   * @param config main Snowplow configuration
   * @param target Redshift storage target configuration
   * @param s3path S3 path to atomic-events folder with shredded TSV files
   * @param transitCopy COPY not straight to events table, but to temporary local table
   * @return valid SQL statement to LOAD
   */
  def buildEventsCopy(config: Config[Redshift], s3path: S3.Folder, transitCopy: Boolean): AtomicCopy = {
    val compressionFormat = getCompressionFormat(config.compression)
    val eventsTable = Common.getEventsTable(config.storage)

    if (transitCopy) {
      AtomicCopy.Transit(SqlString.unsafeCoerce(
        s"""COPY ${Common.TransitEventsTable} FROM '$s3path'
           | CREDENTIALS 'aws_iam_role=${config.storage.roleArn}'
           | REGION AS '${config.region}'
           | MAXERROR ${config.storage.maxError}
           | TIMEFORMAT 'auto'
           | DELIMITER '$EventFieldSeparator'
           | EMPTYASNULL
           | FILLRECORD
           | TRUNCATECOLUMNS
           | ACCEPTINVCHARS $compressionFormat""".stripMargin))
    } else {
      AtomicCopy.Straight(SqlString.unsafeCoerce(
        s"""COPY $eventsTable FROM '$s3path'
           | CREDENTIALS 'aws_iam_role=${config.storage.roleArn}'
           | REGION AS '${config.region}'
           | MAXERROR ${config.storage.maxError}
           | TIMEFORMAT 'auto'
           | DELIMITER '$EventFieldSeparator'
           | EMPTYASNULL
           | FILLRECORD
           | TRUNCATECOLUMNS
           | ACCEPTINVCHARS $compressionFormat""".stripMargin))
    }
  }

  /**
   * Build COPY FROM JSON SQL-statement for shredded types
   *
   * @param config main Snowplow configuration
   * @param tableName valid Redshift table name for shredded type
   * @return valid SQL statement to LOAD
   */
  def buildCopyShreddedStatement(config: Config[_], shreddedType: ShreddedType, tableName: String, maxError: Int, roleArn: String): SqlString = {
    val compressionFormat = getCompressionFormat(config.compression)

    shreddedType match {
      case ShreddedType.Json(_, jsonPathsFile) =>
        SqlString.unsafeCoerce(
          s"""COPY $tableName FROM '${shreddedType.getLoadPath}'
             | CREDENTIALS 'aws_iam_role=$roleArn' JSON AS '$jsonPathsFile'
             | REGION AS '${config.region}'
             | MAXERROR $maxError
             | TIMEFORMAT 'auto'
             | TRUNCATECOLUMNS
             | ACCEPTINVCHARS $compressionFormat""".stripMargin)
      case ShreddedType.Tabular(_) =>
        SqlString.unsafeCoerce(
          s"""COPY $tableName FROM '${shreddedType.getLoadPath}'
             | CREDENTIALS 'aws_iam_role=$roleArn'
             | REGION AS '${config.region}'
             | MAXERROR $maxError
             | TIMEFORMAT 'auto'
             | DELIMITER '$EventFieldSeparator'
             | TRUNCATECOLUMNS
             | ACCEPTINVCHARS $compressionFormat""".stripMargin)
    }
  }

  /**
   * Build ANALYZE SQL-statement
   *
   * @param tableName full (with schema) table name for main or shredded type table
   * @return valid ANALYZE SQL-statement
   */
  def buildAnalyzeStatement(tableName: String): SqlString =
    SqlString.unsafeCoerce(s"ANALYZE $tableName")

  /**
   * Build VACUUM SQL-statement
   *
   * @param tableName full (with schema) table name for main or shredded type table
   * @return valid VACUUM SQL-statement
   */
  def buildVacuumStatement(tableName: String): SqlString =
    SqlString.unsafeCoerce(s"VACUUM SORT ONLY $tableName")

  /** Build APPEND statement for moving data from transit temporary table to atomic events */
  def buildAppendStatement(schema: String): SqlString =
    SqlString.unsafeCoerce(s"ALTER TABLE ${AtomicEvents(schema).getDescriptor} APPEND FROM ${TransitTable(schema).getDescriptor}")

  /** Create a temporary copy of atomic.events table */
  def createTransitTable(schema: String): SqlString = SqlString.unsafeCoerce(
    s"CREATE TABLE ${TransitTable(schema).getDescriptor} ( LIKE ${AtomicEvents(schema).getDescriptor} )")

  /**
    * Destroy the temporary copy of atomic.events table
    * Safe to assume that we're not destroying other table,
    * because table creation would fail whole process
    */
  def destroyTransitTable(schema: String): SqlString = SqlString.unsafeCoerce(
    s"DROP TABLE ${TransitTable(schema).getDescriptor}")

  /**
   * SQL statements for particular shredded type, grouped by their purpose
   *
   * @param copy main COPY FROM statement to load shredded type in its dedicate table
   * @param analyze ANALYZE SQL-statement for dedicated table
   * @param vacuum VACUUM SQL-statement for dedicate table
   */
  private case class ShreddedStatements(copy: SqlString, analyze: SqlString, vacuum: SqlString)

  /**
   * Build group of SQL statements for particular shredded type
   *
   * @param config main Snowplow configuration
   * @param target Redshift storage target configuration
   * @param shreddedType full info about shredded type found in `shredded/good`
   * @return three SQL-statements to load `shreddedType` from S3
   */
  private def transformShreddedType(config: Config[Redshift], shreddedType: ShreddedType): ShreddedStatements = {
    val tableName = config.storage.shreddedTable(ShreddedType.getTableName(shreddedType))
    val copyFromJson = buildCopyShreddedStatement(config, shreddedType, tableName, config.storage.maxError, config.storage.roleArn)
    val analyze = buildAnalyzeStatement(tableName)
    val vacuum = buildVacuumStatement(tableName)
    ShreddedStatements(copyFromJson, analyze, vacuum)
  }

  /**
   * Stringify output codec to use in SQL statement
   */
  private def getCompressionFormat(outputCodec: Config.OutputCompression): String = outputCodec match {
    case Config.OutputCompression.None => ""
    case Config.OutputCompression.Gzip => "GZIP"
  }
}
