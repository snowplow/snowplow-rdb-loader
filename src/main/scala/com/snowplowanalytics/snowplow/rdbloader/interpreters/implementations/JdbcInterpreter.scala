/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader
package interpreters.implementations

import java.io.FileReader
import java.nio.file.Path
import java.sql.{Connection, SQLException}
import java.util.Properties

import scala.util.control.NonFatal

import com.amazon.redshift.jdbc42.{Driver => RedshiftDriver}

import cats.implicits._

import org.postgresql.copy.CopyManager
import org.postgresql.jdbc.PgConnection
import org.postgresql.{Driver => PgDriver}

import LoaderError.StorageTargetError
import db.Decoder
import config.StorageTarget
import loaders.Common.SqlString

object JdbcInterpreter {

  /**
   * Execute a single update-statement in provided Postgres connection
   *
   * @param conn Postgres connection
   * @param sql string with valid SQL statement
   * @return number of updated rows in case of success, failure otherwise
   */
  def executeUpdate(conn: Connection)(sql: SqlString): Either[StorageTargetError, Long] =
    Either.catchNonFatal {
      conn.createStatement().executeUpdate(sql).toLong
    } leftMap {
      case NonFatal(e: java.sql.SQLException) if Option(e.getMessage).getOrElse("").contains("is not authorized to assume IAM Role") =>
        StorageTargetError("IAM Role with S3 Read permissions is not attached to Redshift instance")
      case NonFatal(e) =>
        System.err.println("RDB Loader unknown error in executeUpdate")
        e.printStackTrace()
        StorageTargetError(Option(e.getMessage).getOrElse(e.toString))
    }

  def executeQuery[A](conn: Connection)(sql: SqlString)(implicit ev: Decoder[A]): Either[StorageTargetError, A] =
    try {
      val resultSet = conn.createStatement().executeQuery(sql)
      ev.decode(resultSet) match {
        case Left(e) => StorageTargetError(s"Cannot decode SQL row: ${e.message}").asLeft
        case Right(a) =>
          println(a)
          a.asRight[StorageTargetError]
      }
    } catch {
      case NonFatal(e) =>
        System.err.println("RDB Loader unknown error in executeQuery")
        e.printStackTrace()
        StorageTargetError(Option(e.getMessage).getOrElse(e.toString)).asLeft[A]
    }

  def setAutocommit(conn: Connection, autoCommit: Boolean): Either[LoaderError, Unit] =
    try {
      Right(conn.setAutoCommit(autoCommit))
    } catch {
      case e: SQLException =>
        System.err.println("setAutocommit error")
        e.printStackTrace()
        Left(StorageTargetError(e.toString))
    }

  def copyViaStdin(conn: Connection, files: List[Path], copyStatement: SqlString): Either[LoaderError, Long] = {
    val copyManager = Either.catchNonFatal {
      new CopyManager(conn.asInstanceOf[PgConnection])
    } leftMap { e => StorageTargetError(e.toString) }

    for {
      manager <- copyManager
      _ <- setAutocommit(conn, false)
      result = files.traverse(copyIn(manager, copyStatement)(_)).map(_.combineAll)
      _ = result.fold(_ => conn.rollback(), _ => conn.commit())
      _ <- setAutocommit(conn, true)
      endResult <- result
    } yield endResult
  }

  def copyIn(copyManager: CopyManager, copyStatement: String)(file: Path): Either[LoaderError, Long] =
    try {
      Right(copyManager.copyIn(copyStatement, new FileReader(file.toFile)))
    } catch {
      case NonFatal(e) => Left(StorageTargetError(e.toString))
    }

  /**
   * Get Redshift or Postgres connection
   */
  def getConnection(target: StorageTarget): Either[LoaderError, Connection] = {
    try {
      val password = target.password match {
        case StorageTarget.PlainText(text) => text
        case StorageTarget.EncryptedKey(StorageTarget.EncryptedConfig(key)) =>
          SshInterpreter.getKey(key.parameterName).getOrElse(throw new RuntimeException("Cannot retrieve JDBC password from EC2 Parameter Store"))
      }

      val props = new Properties()
      props.setProperty("user", target.username)
      props.setProperty("password", password)

      target match {
        case r: StorageTarget.RedshiftConfig =>
          val url = s"jdbc:redshift://${target.host}:${target.port}/${target.database}"
          for {
            _ <- r.jdbc.validation match {
              case Left(error) => error.asLeft
              case Right(propertyUpdaters) =>
                propertyUpdaters.foreach(f => f(props)).asRight
            }
            connection <- Either.catchNonFatal(new RedshiftDriver().connect(url, props)).leftMap { x =>
              LoaderError.StorageTargetError(x.getMessage)
            }
          } yield connection

        case p: StorageTarget.PostgresqlConfig =>
          val url = s"jdbc:postgresql://${p.host}:${p.port}/${p.database}"
          props.setProperty("sslmode", p.sslMode.asProperty)
          Right(new PgDriver().connect(url, props))
      }
    } catch {
      case NonFatal(e) =>
        System.err.println("RDB Loader getConnection error")
        e.printStackTrace()
        Left(StorageTargetError(s"Problems with establishing DB connection\n${e.getMessage}"))
    }
  }
}
