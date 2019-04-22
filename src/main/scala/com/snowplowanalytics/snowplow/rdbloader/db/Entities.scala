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
                              shreddedCardinality: Int) {
    def show: String =
      s"ETL timestamp $etlTstamp with $eventCount events and $shreddedCardinality shredded types, commited at $commitTstamp"
  }
}
