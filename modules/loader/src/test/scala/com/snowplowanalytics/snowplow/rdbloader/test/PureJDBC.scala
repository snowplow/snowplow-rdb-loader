package com.snowplowanalytics.snowplow.rdbloader.test

import java.time.Instant
import java.sql.Timestamp
import cats.implicits._
import doobie.{ConnectionIO, Query0, Read}
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.rdbloader.core.db.{Statement => SqlStatement}
import com.snowplowanalytics.snowplow.rdbloader.core.algebras.JDBC
import com.snowplowanalytics.snowplow.rdbloader.redshift.db.Statement
import com.snowplowanalytics.snowplow.rdbloader.core.{LoaderAction, LoaderError}

case class PureJDBC(
  executeQuery: SqlStatement  => LoaderAction[Pure, Any],
  executeUpdate: SqlStatement => LoaderAction[Pure, Int]
) {

  /** If certain predicate met, return `update` action, otherwise do usual `executeUpdate` */
  def withExecuteUpdate(predicate: (SqlStatement, TestState) => Boolean, update: LoaderAction[Pure, Int]): PureJDBC = {
    val updated = (sql: SqlStatement) => {
      Pure { (ts: TestState) =>
        if (predicate(sql, ts))
          (ts, update)
        else (ts, executeUpdate(sql))
      }.toAction.flatMap(identity)
    }
    this.copy(executeUpdate = updated)
  }
}

object PureJDBC {

  def getResult(s: TestState)(query: SqlStatement): Any =
    query match {
      case Statement.GetVersion(_, _)        => SchemaKey("com.acme", "some_context", "jsonschema", SchemaVer.Full(2, 0, 0))
      case Statement.TableExists(_, _)       => false
      case Statement.GetColumns(_)           => List("some_column")
      case Statement.GetLoadTstamp(_, _)     => Some(Timestamp.from(Instant.ofEpochMilli(s.time)))
      case Statement.ManifestRead(_, _)      => None
      case Statement.FoldersMinusManifest(_) => List()
      case _                                 => throw new IllegalArgumentException(s"Unexpected query $query with ${s.getLog}")
    }

  val init: PureJDBC = custom(getResult)

  def custom(getResult: TestState => SqlStatement => Any): PureJDBC = {
    def executeQuery(query: SqlStatement): LoaderAction[Pure, Any] =
      Pure((s: TestState) => (s.log(query), getResult(s)(query).asInstanceOf[Any].asRight[LoaderError])).toAction

    def executeUpdate(sql: SqlStatement): LoaderAction[Pure, Int] =
      Pure((s: TestState) => (s.log(sql), 1.asRight[LoaderError])).toAction

    PureJDBC(q => executeQuery(q), executeUpdate)
  }

  def interpreter(results: PureJDBC): JDBC[Pure] = new JDBC[Pure] {
    def executeUpdate(sql: SqlStatement): LoaderAction[Pure, Int] =
      results.executeUpdate(sql)

    def query[G[_], A](get: Query0[A] => ConnectionIO[G[A]], sql: Query0[A]): Pure[Either[LoaderError, G[A]]] =
      throw new NotImplementedError("query method in testing JDBC interpreter")

    override def setAutoCommit(a: Boolean): Pure[Unit] =
      Pure.unit

    override def executeQuery[A](query: SqlStatement)(implicit A: Read[A]): LoaderAction[Pure, A] =
      results.executeQuery.asInstanceOf[SqlStatement => LoaderAction[Pure, A]](query)

    override def executeQueryList[A](query: SqlStatement)(implicit A: Read[A]): LoaderAction[Pure, List[A]] =
      results.executeQuery.asInstanceOf[SqlStatement => LoaderAction[Pure, List[A]]](query)

    override def executeQueryOption[A](query: SqlStatement)(implicit A: Read[A]): LoaderAction[Pure, Option[A]] =
      results.executeQuery.asInstanceOf[SqlStatement => LoaderAction[Pure, Option[A]]](query)

  }
}
