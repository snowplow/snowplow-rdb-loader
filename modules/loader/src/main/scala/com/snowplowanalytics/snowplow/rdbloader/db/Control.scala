package com.snowplowanalytics.snowplow.rdbloader.db

import cats.{Functor, Monad}
import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.dsl.JDBC
import com.snowplowanalytics.snowplow.rdbloader.{LoaderAction, LoaderError}

import com.snowplowanalytics.iglu.schemaddl.redshift.generators.DdlFile
import com.snowplowanalytics.iglu.schemaddl.redshift.{AlterTable, RenameTo}

/** Set of common functions to control DB entities */
object Control {
  def renameTable[F[_]: Functor: JDBC](schema: String, from: String, to: String): LoaderAction[F, Unit] = {
    val alterTable = DdlFile(List(AlterTable(s"$schema.$from", RenameTo(to))))
    JDBC[F].executeUpdate(Statement.DdlFile(alterTable)).void
  }

  def tableExists[F[_]: Functor: JDBC](dbSchema: String, tableName: String): LoaderAction[F, Boolean] =
    JDBC[F].executeQuery[Boolean](Statement.TableExists(dbSchema, tableName)).leftMap(annotateError(dbSchema, tableName))

  def schemaExists[F[_]: Functor: JDBC](dbSchema: String): LoaderAction[F, Boolean] =
    JDBC[F].executeQueryOption[String](Statement.SchemaExists(dbSchema)).map(_.isDefined)

  def withTransaction[F[_]: Monad: JDBC, A](loaderAction: LoaderAction[F, A]): LoaderAction[F, A] = {
    val action = loaderAction.value.flatMap {
      case Right(a) => JDBC[F].executeUpdate(Statement.Commit).as(a).value
      case Left(error) => JDBC[F].executeUpdate(Statement.Abort).value.as(error.asLeft[A])
    }
    JDBC[F].executeUpdate(Statement.Begin) *> LoaderAction(action)
  }

  /** List all columns in the table */
  def getColumns[F[_]: Monad: JDBC](dbSchema: String, tableName: String): LoaderAction[F, List[String]] =
    for {
      _       <- JDBC[F].executeUpdate(Statement.SetSchema(dbSchema))
      columns <- JDBC[F].executeQueryList[String](Statement.GetColumns(tableName)).leftMap(annotateError(dbSchema, tableName))
    } yield columns


  def annotateError(dbSchema: String, tableName: String)(error: LoaderError): LoaderError =
    error match {
      case LoaderError.StorageTargetError(message) =>
        LoaderError.StorageTargetError(s"$dbSchema.$tableName. " ++ message)
      case other =>
        other
    }
}
