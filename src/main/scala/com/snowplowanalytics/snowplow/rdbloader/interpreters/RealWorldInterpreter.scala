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
package interpreters

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.sql.Connection

import scala.util.control.NonFatal

import cats._
import cats.implicits._

import io.circe.Json

import com.amazonaws.services.s3.AmazonS3

import com.snowplowanalytics.iglu.client.Client

import org.joda.time.DateTime

import com.snowplowanalytics.snowplow.scalatracker.Tracker

import com.snowplowanalytics.manifest.core.ManifestError

// This project
import LoaderA._
import LoaderError.LoaderLocalError
import Interpreter.runIO
import config.CliConfig
import discovery.ManifestDiscovery
import utils.Common
import implementations._
import com.snowplowanalytics.snowplow.rdbloader.{ Log => ExitLog }
import com.snowplowanalytics.snowplow.rdbloader.loaders.Common.SqlString

/**
 * Interpreter performs all actual side-effecting work,
 * interpreting `Action` at the end-of-the-world.
 * It contains and handles configuration, connections and mutable state,
 * all real-world interactions, except argument parsing
 */
class RealWorldInterpreter private[interpreters](
  cliConfig: CliConfig,
  amazonS3: AmazonS3,
  tracker: Option[Tracker],
  resolver: Client[Id, Json]) extends Interpreter {

  private val interpreter = this

  /**
    * Successfully fetched JSONPaths
    * Key: "vendor/filename_1.json";
    * Value: "s3://my-jsonpaths/redshift/vendor/filename_1.json"
    */
  private val cache = collection.mutable.HashMap.empty[String, Option[S3.Key]]

  // dbConnection is Either because not required for log dump
  // lazy to wait before tunnel established
  private var dbConnection: Either[LoaderError, Connection] = _

  private def getConnection(force: Boolean = false): Either[LoaderError, Connection] = {
    if (dbConnection == null) {
      dbConnection = JdbcInterpreter.getConnection(cliConfig.target)
    }
    if (force) {
      println("Forcing reconnection to DB")
      dbConnection = JdbcInterpreter.getConnection(cliConfig.target)
    }
    dbConnection
  }

  private lazy val manifest =
    ManifestInterpreter.initialize(cliConfig.target.processingManifest, cliConfig.configYaml.aws.s3.region, utils.Common.DefaultClient) match {
      case Right(Some(m)) => m.asRight
      case Right(None) => LoaderLocalError("Processing Manifest is not configured").asLeft
      case Left(error) => error.asLeft
    }

  // General messages that should be printed both to output and final log
  private val messages = collection.mutable.ListBuffer.empty[String]

  // DB messages that should be printed only to output and if failure is DB-related
  private val messagesCopy = collection.mutable.ListBuffer.empty[String]

  def executeWithRetry[A](action: Connection => SqlString => Either[LoaderError.StorageTargetError, A])(sql: SqlString) = {
    val firstAttempt = for { conn <- getConnection(); r <- action(conn)(sql) } yield r
    firstAttempt match {
      case Left(LoaderError.StorageTargetError(message)) if message.contains("Connection refused") =>
        println(message)
        println("Sleeping and making another try")
        Thread.sleep(10000)
        for {
          conn <- getConnection(true)
          r <- action(conn)(sql)
        } yield r
      case other => other
    }
  }

  def run: LoaderA ~> Id = new (LoaderA ~> Id) {

    def apply[A](effect: LoaderA[A]): Id[A] = {
      effect match {
        case ListS3(folder) =>
          log(s"Listing $folder")
          S3Interpreter.list(amazonS3, folder).map(summaries => summaries.map(S3.getKey))
        case KeyExists(key) =>
          S3Interpreter.keyExists(amazonS3, key)
        case DownloadData(source, dest) =>
          S3Interpreter.downloadData(amazonS3, source, dest)


        case ManifestDiscover(loader, shredder, predicate) =>
          for {
            manifestClient <- manifest
            result <- ManifestInterpreter.getUnprocessed(manifestClient, loader, shredder, predicate) match {
              case Right(result) if result.isEmpty =>
                log(s"No new items discovered in processing manifest")
                result.asRight
              case Right(h :: Nil) =>
                log(s"Single ${h.id} item discovered")
                List(h).asRight
              case Right(result) =>
                log(s"Multiple (${result.length}) items discovered")
                result.asRight
              case other => other
            }
          } yield result
        case ManifestProcess(item, load) =>
          for {
            manifestClient <- manifest
            process = ManifestInterpreter.process(interpreter, load)
            app = ManifestDiscovery.getLoaderApp(cliConfig.target.id)
            _ <- runIO(manifestClient.processItem(app, None, process)(item).leftMap {
              case ManifestError.ApplicationError(e, _, _) =>
                val message = Option(e.getMessage).getOrElse(e.toString)
                LoaderError.StorageTargetError(message)
              case e => LoaderError.fromManifestError(e)
            }.value)
          } yield ()

        case ExecuteUpdate(query) =>
          if (query.startsWith("COPY ")) { logCopy(query.split(" ").take(2).mkString(" ")) }
          executeWithRetry[Long](JdbcInterpreter.executeUpdate)(query).asInstanceOf[Id[A]]

        case CopyViaStdin(files, query) =>
          for {
            conn <- getConnection()
            _ = log(s"Copying ${files.length} files via stdin")
            res <- JdbcInterpreter.copyViaStdin(conn, files, query)
          } yield res

        case ExecuteQuery(query, d) =>
          for {
            conn <- getConnection()
            res <- JdbcInterpreter.executeQuery(conn)(query)(d)
          } yield res

        case CreateTmpDir =>
          try {
            Files.createTempDirectory("rdb-loader").asRight
          } catch {
            case NonFatal(e) =>
              LoaderLocalError("Cannot create temporary directory.\n" + e.toString).asLeft
          }
        case DeleteDir(path) =>
          try {
            Files.walkFileTree(path, RealWorldInterpreter.DeleteVisitor).asRight[LoaderError].void
          } catch {
            case NonFatal(e) => LoaderLocalError(s"Cannot delete directory [${path.toString}].\n" + e.toString).asLeft
          }

        case Print(message) =>
          log(message)
        case Sleep(timeout) =>
          log(s"Sleeping $timeout milliseconds")
          Thread.sleep(timeout)
        case Track(result) =>
          result match {
            case ExitLog.LoadingSucceeded =>
              TrackerInterpreter.trackSuccess(tracker)
              log(result.toString)
            case ExitLog.LoadingFailed(message) =>
              val secrets = List(cliConfig.target.password.getUnencrypted, cliConfig.target.username)
              val sanitizedMessage = Common.sanitize(message, secrets)
              TrackerInterpreter.trackError(tracker)
              log(sanitizedMessage)
          }
        case Dump(key) =>
          log(s"Dumping $key")
          val logs = messages.mkString("\n") + "\n"
          TrackerInterpreter.dumpStdout(amazonS3, key, logs)
        case Exit(loadResult, dumpResult) =>
          getConnection().foreach(c => c.close())
          TrackerInterpreter.exit(loadResult, dumpResult)


        case Get(key: String) =>
          cache.get(key)
        case Put(key: String, value: Option[S3.Key]) =>
          val _ = cache.put(key, value)
          ()

        case EstablishTunnel(config) =>
          log("Establishing SSH tunnel")
          SshInterpreter.establishTunnel(config)
        case CloseTunnel() =>
          log("Closing SSH tunnel")
          SshInterpreter.closeTunnel()

        case GetEc2Property(name) =>
          SshInterpreter.getKey(name)
      }
    }
  }


  override def getLastCopyStatements: String = {
    val last = messagesCopy.take(3)
    if (last.isEmpty)
      "No COPY statements were performed"
    else
      s"Last ${last.length} COPY statements:\n${last.mkString("\n")}"
  }

  private def log(message: String): Unit = {
    val endMessage = s"RDB Loader [${DateTime.now()}]: $message"
    System.out.println(message)
    messages.append(endMessage)
  }

  private def logCopy(message: String): Unit = {
    val endMessage = s"RDB Loader [${DateTime.now()}]: $message"
    System.out.println(endMessage)
    messagesCopy.append(endMessage)
  }

}

object RealWorldInterpreter {

  object DeleteVisitor extends SimpleFileVisitor[Path] {
    override def visitFile(file: Path, attrs: BasicFileAttributes) = {
      Files.delete(file)
      FileVisitResult.CONTINUE
    }

    override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
      Files.delete(dir)
      FileVisitResult.CONTINUE
    }
  }
}
