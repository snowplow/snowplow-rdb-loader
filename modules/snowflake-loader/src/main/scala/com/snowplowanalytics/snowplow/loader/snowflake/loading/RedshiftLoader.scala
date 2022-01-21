package com.snowplowanalytics.snowplow.loader.snowflake.loading

import cats.Monad
import cats.effect.{Effect, LiftIO}
import cats.syntax.all._
import cats.effect.syntax.all._
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import com.snowplowanalytics.snowplow.rdbloader.loading.Stage
import com.snowplowanalytics.snowplow.rdbloader.state.Control
import com.snowplowanalytics.snowplow.loader.snowflake.config.RedshiftTarget
import com.snowplowanalytics.snowplow.loader.snowflake.db.{RsDao, SfDao, Statement}
import com.snowplowanalytics.snowplow.loader.snowflake.loading.RedshiftStatements.getStatements
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.TargetLoader

class RedshiftLoader[
  F[_]: Effect: Control,
  C[_]: Monad: Logging: SfDao: LiftIO
] extends TargetLoader[F, C] {

  /**
    * Run loading actions for atomic and shredded data
    *
    * @param discovery batch discovered from message queue
    * @return block of VACUUM and ANALYZE statements to execute them out of a main transaction
    */
  def run(discovery: DataDiscovery): C[Unit] = ???

}
