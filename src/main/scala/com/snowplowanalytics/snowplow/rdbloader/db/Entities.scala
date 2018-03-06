package com.snowplowanalytics.snowplow.rdbloader.db

import java.sql.{ Timestamp => SqlTimestamp }

/** Different entities that are queried from database using `Decoder`s */
object Entities {

  /** Count query */
  case class Count(count: Long)

  case class Timestamp(etlTstamp: SqlTimestamp)

  case class LoadManifestItem(etlTstamp: SqlTimestamp,
                              commitTstamp: SqlTimestamp,
                              eventCount: Int,
                              shreddedCardinality: Int)
}
