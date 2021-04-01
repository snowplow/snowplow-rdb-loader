package com.snowplowanalytics.snowplow.rdbloader.test

import cats.implicits._

import doobie.{Query0, Read, ConnectionIO}

import com.snowplowanalytics.iglu.core.{SchemaVer, SchemaKey}

import com.snowplowanalytics.snowplow.rdbloader.db.Statement
import com.snowplowanalytics.snowplow.rdbloader.{LoaderError, LoaderAction}
import com.snowplowanalytics.snowplow.rdbloader.dsl.JDBC

case class PureJDBC(executeQuery: Statement => LoaderAction[Pure, Any],
                    executeUpdate: Statement => LoaderAction[Pure, Int]) {
  /** If certain predicate met, return `update` action, otherwise do usual `executeUpdate` */
  def withExecuteUpdate(predicate: (Statement, TestState) => Boolean, update: LoaderAction[Pure, Int]): PureJDBC = {
    val updated = (sql: Statement) => {
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

  def getResult(s: TestState)(query: Statement): Any =
    query match {
      case Statement.SchemaExists("public") => Some("public")
      case Statement.GetVersion(_, _) => SchemaKey("com.acme", "some_context", "jsonschema", SchemaVer.Full(2,0,0))
      case Statement.TableExists(_, _) => false
      case Statement.GetColumns(_) => List("some_column")
      case Statement.ManifestGet(_, _) => None
      case _ => throw new IllegalArgumentException(s"Unexpected query $query with ${s.getLog}")
    }

  val init: PureJDBC = custom(getResult)

  def custom(getResult: TestState => Statement => Any): PureJDBC = {
    def executeQuery(query: Statement): LoaderAction[Pure, Any] = {
      Pure((s: TestState) => (s.log(query), getResult(s)(query).asInstanceOf[Any].asRight[LoaderError])).toAction
    }

    def executeUpdate(sql: Statement): LoaderAction[Pure, Int] =
      Pure((s: TestState) => (s.log(sql), 1.asRight[LoaderError])).toAction

    PureJDBC(q => executeQuery(q), executeUpdate)
  }

  def interpreter(results: PureJDBC): JDBC[Pure] = new JDBC[Pure] {
    def executeUpdate(sql: Statement): LoaderAction[Pure, Int] =
      results.executeUpdate(sql)

    def query[G[_], A](get: Query0[A] => ConnectionIO[G[A]], sql: Query0[A]): Pure[Either[LoaderError, G[A]]] =
      throw new NotImplementedError("query method in testing JDBC interpreter")

    override def executeQuery[A](query: Statement)(implicit A: Read[A]): LoaderAction[Pure, A] =
      results.executeQuery.asInstanceOf[Statement => LoaderAction[Pure, A]](query)

    override def executeQueryList[A](query: Statement)(implicit A: Read[A]): LoaderAction[Pure, List[A]] =
      results.executeQuery.asInstanceOf[Statement => LoaderAction[Pure, List[A]]](query)

    override def executeQueryOption[A](query: Statement)(implicit A: Read[A]): LoaderAction[Pure, Option[A]] =
      results.executeQuery.asInstanceOf[Statement => LoaderAction[Pure, Option[A]]](query)

  }
}

