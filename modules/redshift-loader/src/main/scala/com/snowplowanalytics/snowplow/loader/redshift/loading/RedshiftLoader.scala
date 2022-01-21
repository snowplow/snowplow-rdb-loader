package com.snowplowanalytics.snowplow.loader.redshift.loading

import cats.Monad
import cats.syntax.all._
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import com.snowplowanalytics.snowplow.rdbloader.loading.Stage
import com.snowplowanalytics.snowplow.rdbloader.state.Control
import com.snowplowanalytics.snowplow.loader.redshift.config.RedshiftTarget
import com.snowplowanalytics.snowplow.loader.redshift.db.{RsDao, Statement}
import com.snowplowanalytics.snowplow.loader.redshift.loading.RedshiftStatements.getStatements
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.TargetLoader

class RedshiftLoader[C[_]: Monad: Logging: RsDao: Control](target: RedshiftTarget, region: String)
    extends TargetLoader[C] {

  /**
    * Run loading actions for atomic and shredded data
    *
    * @param discovery batch discovered from message queue
    * @return block of VACUUM and ANALYZE statements to execute them out of a main transaction
    */
  def run(discovery: DataDiscovery): C[Unit] =
    for {
      _ <- Logging[C].info(s"Loading ${discovery.base}")
      statements = getStatements(target, region, discovery)
      _ <- loadFolder(statements)
      _ <- Logging[C].info(s"Folder [${discovery.base}] has been loaded (not committed yet)")
    } yield ()

  /** Perform data-loading for a single run folder */
  def loadFolder(statements: RedshiftStatements): C[Unit] =
    Control[C].setStage(Stage.Loading("events")) *>
      loadAtomic(statements.dbSchema, statements.atomicCopy) *>
      statements.shredded.traverse_ { statement =>
        Logging[C].info(statement.title) *>
          Control[C].setStage(Stage.Loading(statement.shreddedType.getTableName)) *>
          RsDao[C].executeUpdate(statement).void
      }

  /** Get COPY action, either straight or transit (along with load manifest check) atomic.events copy */
  def loadAtomic(dbSchema: String, copy: Statement.EventsCopy): C[Unit] =
    if (copy.transitCopy)
      Logging[C].info(s"COPY $dbSchema.events (transit)") *>
        RsDao[C].executeUpdate(Statement.CreateTransient(dbSchema)) *>
        RsDao[C].executeUpdate(copy) *>
        RsDao[C].executeUpdate(Statement.DropTransient(dbSchema)).void
    else
      Logging[C].info(s"COPY $dbSchema.events") *>
        RsDao[C].executeUpdate(copy).void

}
