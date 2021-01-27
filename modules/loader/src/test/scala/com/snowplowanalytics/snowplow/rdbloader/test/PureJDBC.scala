package com.snowplowanalytics.snowplow.rdbloader.test

import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.dsl.JDBC
import com.snowplowanalytics.snowplow.rdbloader.db.Decoder
import com.snowplowanalytics.iglu.core.{SchemaVer, SchemaKey}

import com.snowplowanalytics.snowplow.rdbloader.loading.Load.SqlString
import com.snowplowanalytics.snowplow.rdbloader.{LoaderAction, LoaderError}
import com.snowplowanalytics.snowplow.rdbloader.db.Entities.{TableState, Columns}

case class PureJDBC(executeQuery: SqlString => Decoder[Any] => LoaderAction[Pure, Any],
                    executeUpdate: SqlString => LoaderAction[Pure, Long]) {
  /** If certain predicate met, return `update` action, otherwise do usual `executeUpdate` */
  def withExecuteUpdate(predicate: (SqlString, TestState) => Boolean, update: LoaderAction[Pure, Long]): PureJDBC = {
    val updated = (sql: SqlString) => {
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
    def executeQuery[A](query: SqlString)(implicit ev: Decoder[A]): LoaderAction[Pure, A] = {
      val result = ev.name match {
        case "TableState" => TableState(SchemaKey("com.acme", "some_context", "jsonschema", SchemaVer.Full(2,0,0)))
        case "Boolean" => false
        case "Columns" => Columns(List("some_column"))
      }
      Pure((s: TestState) => (s.log(query), result.asInstanceOf[A].asRight[LoaderError])).toAction
    }

    def executeUpdate(sql: SqlString): LoaderAction[Pure, Long] =
      Pure((s: TestState) => (s.log(sql), 1L.asRight[LoaderError])).toAction

    PureJDBC(q => e => executeQuery(q)(e), executeUpdate)
  }

  def interpreter(results: PureJDBC): JDBC[Pure] = new JDBC[Pure] {
    def executeUpdate(sql: SqlString): LoaderAction[Pure, Long] =
      results.executeUpdate(sql)

    def executeQuery[A](query: SqlString)(implicit ev: Decoder[A]): LoaderAction[Pure, A] =
      results.executeQuery.asInstanceOf[SqlString => Decoder[A] => LoaderAction[Pure, A]](query)(ev)
  }
}

