package com.snowplowanalytics.snowplow.loader.snowflake

import com.snowplowanalytics.snowplow.rdbloader.db.helpers.DAO

package object db {
  type SfDao[C[_]] = DAO[C, ???]

  object RsDao {
    def apply[C[_]](implicit ev: SfDao[C]): SfDao[C] = ev
  }
}
