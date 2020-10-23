package com.snowplowanalytics.snowplow.rdbloader

import java.nio.file.Path

import cats.effect.IO
import cats.syntax.all._
import cats.effect.concurrent.Ref

import com.snowplowanalytics.snowplow.rdbloader.db.Decoder
import com.snowplowanalytics.snowplow.rdbloader.dsl.JDBC
import com.snowplowanalytics.snowplow.rdbloader.loaders.Common.SqlString

object Interpreters {


  def ioTestJdbcInterpreter(queries: Ref[IO, List[String]]): JDBC[IO] = new JDBC[IO] {
    def executeUpdate(sql: SqlString): LoaderAction[IO, Long] = {
      val action = queries
        .update(qs => sql.split(" ").headOption.toList.map(_.trim) ++ qs)
        .as(1L.asRight[LoaderError])
      LoaderAction[IO, Long](action)
    }

    def executeQuery[A](query: SqlString)(implicit ev: Decoder[A]): LoaderAction[IO, A] = {
      val action = queries
        .update(qs => query.split(" ").headOption.toList.map(_.trim) ++ qs)
        .as("".asInstanceOf[A].asRight[LoaderError])
      LoaderAction[IO, A](action)
    }
  }
}

