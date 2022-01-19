package com.snowplowanalytics.snowplow.loader.snowflake.loading

import cats.Monad
import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import com.snowplowanalytics.snowplow.rdbloader.state.Control
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.TargetLoader
import com.snowplowanalytics.snowplow.loader.snowflake.db.SfDao
import com.snowplowanalytics.snowplow.loader.snowflake.config.SnowflakeTarget

class SnowflakeLoader[C[_]: Monad: Logging: SfDao: Control](target: SnowflakeTarget, region: String)
  extends TargetLoader[C] {

  /**
    * Run loading actions for atomic and shredded data
    *
    * @param discovery batch discovered from message queue
    * @return block of VACUUM and ANALYZE statements to execute them out of a main transaction
    */
  def run(discovery: DataDiscovery): C[Unit] =
    Logging[C].info(target.username) *> Logging[C].info(region)

}
