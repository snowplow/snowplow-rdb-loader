package com.snowplowanalytics.snowplow.rdbloader.test

import cats.implicits._

import doobie.Read

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

  val init: PureJDBC = {
    def executeQuery(query: Statement): LoaderAction[Pure, Any] = {
      val result = query match {
        case Statement.GetVersion(_, _) => SchemaKey("com.acme", "some_context", "jsonschema", SchemaVer.Full(2,0,0))
        case Statement.TableExists(_, _) => false
        case Statement.GetColumns(_) => List("some_column")
        case _ => throw new IllegalArgumentException(s"Unexpected query $query")
      }
      Pure((s: TestState) => (s.log(query), result.asInstanceOf[Any].asRight[LoaderError])).toAction
    }

    def executeUpdate(sql: Statement): LoaderAction[Pure, Int] =
      Pure((s: TestState) => (s.log(sql), 1.asRight[LoaderError])).toAction

    PureJDBC(q => executeQuery(q), executeUpdate)
  }

  def interpreter(results: PureJDBC): JDBC[Pure] = new JDBC[Pure] {
    def executeUpdate(sql: Statement): LoaderAction[Pure, Int] =
      results.executeUpdate(sql)

    def executeQuery[A](query: Statement)(implicit A: Read[A]): LoaderAction[Pure, A] =
      results.executeQuery.asInstanceOf[Statement => LoaderAction[Pure, A]](query)

    def executeQueryList[A](query: Statement)(implicit A: Read[A]): LoaderAction[Pure, List[A]] =
      results.executeQuery.asInstanceOf[Statement => LoaderAction[Pure, List[A]]](query)

  }
}

