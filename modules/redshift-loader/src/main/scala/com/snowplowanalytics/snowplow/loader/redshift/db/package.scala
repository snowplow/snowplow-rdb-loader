package com.snowplowanalytics.snowplow.loader.redshift

import com.snowplowanalytics.snowplow.rdbloader.db.helpers.DAO

package object db {
  type RsDao[C[_]] = DAO[C, Statement]

  object RsDao {
    def apply[C[_]](implicit ev: RsDao[C]): RsDao[C] = ev
  }
}
