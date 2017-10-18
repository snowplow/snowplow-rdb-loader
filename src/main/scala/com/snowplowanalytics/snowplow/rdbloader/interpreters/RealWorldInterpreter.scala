/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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

import scala.util.control.NonFatal

import cats._
import cats.implicits._

import com.amazonaws.services.s3.AmazonS3

import com.snowplowanalytics.snowplow.scalatracker.Tracker

// This project
import config.CliConfig
import LoaderA._
import LoaderError.LoaderLocalError
import utils.Common
import implementations.{PgInterpreter, S3Interpreter, SshInterpreter, TrackerInterpreter}
import com.snowplowanalytics.snowplow.rdbloader.{ Log => ExitLog }

/**
 * Interpreter performs all actual side-effecting work,
 * interpreting `Action` at the end-of-the-world.
 * It contains and handles configuration, connections and mutable state,
 * all real-world interactions, except argument parsing
 */
class RealWorldInterpreter private[interpreters](
  cliConfig: CliConfig,
  amazonS3: AmazonS3,
  tracker: Option[Tracker]) extends Interpreter {

  /**
    * Successfully fetched JSONPaths
    * Key: "vendor/filename_1.json";
    * Value: "s3://my-jsonpaths/redshift/vendor/filename_1.json"
    */
  private val cache = collection.mutable.HashMap.empty[String, Option[S3.Key]]

  // dbConnection is Either because not required for log dump
  // lazy to wait before tunnel established
  private lazy val dbConnection = PgInterpreter.getConnection(cliConfig.target)

  def run: LoaderA ~> Id = new (LoaderA ~> Id) {

    def apply[A](effect: LoaderA[A]): Id[A] = {
      effect match {
        case ListS3(folder) =>
          S3Interpreter.list(amazonS3, folder).map(summaries => summaries.map(S3.getKey))
        case KeyExists(key) =>
          S3Interpreter.keyExists(amazonS3, key)
        case DownloadData(source, dest) =>
          S3Interpreter.downloadData(amazonS3, source, dest)

        case ExecuteQuery(query) =>
          for {
            conn <- dbConnection
            res <- PgInterpreter.executeQuery(conn)(query)
          } yield res
        case CopyViaStdin(files, query) =>
          for {
            conn <- dbConnection
            res <- PgInterpreter.copyViaStdin(conn, files, query)
          } yield res

        case CreateTmpDir =>
          try {
            Files.createTempDirectory("rdb-loader").asRight
          } catch {
            case NonFatal(e) => LoaderLocalError("Cannot create temporary directory.\n" + e.toString).asLeft
          }
        case DeleteDir(path) =>
          try {
            Files.walkFileTree(path, RealWorldInterpreter.DeleteVisitor).asRight[LoaderError].void
          } catch {
            case NonFatal(e) => LoaderLocalError(s"Cannot delete directory [${path.toString}].\n" + e.toString).asLeft
          }

        case Sleep(timeout) =>
          Thread.sleep(timeout)
        case Track(result) =>
          result match {
            case ExitLog.LoadingSucceeded(_) =>
              TrackerInterpreter.trackSuccess(tracker)
            case ExitLog.LoadingFailed(message, _) =>
              val secrets = List(cliConfig.target.password.getUnencrypted, cliConfig.target.username)
              val sanitizedMessage = Common.sanitize(message, secrets)
              TrackerInterpreter.trackError(tracker, sanitizedMessage)
          }
        case Dump(key, result) =>
          TrackerInterpreter.dumpStdout(amazonS3, key, result.toString)
        case Exit(loadResult, dumpResult) =>
          dbConnection.foreach(c => c.close())
          TrackerInterpreter.exit(loadResult, dumpResult)


        case Get(key: String) =>
          cache.get(key)
        case Put(key: String, value: Option[S3.Key]) =>
          val _ = cache.put(key, value)
          ()

        case EstablishTunnel(config) =>
          SshInterpreter.establishTunnel(config)
        case CloseTunnel() =>
          SshInterpreter.closeTunnel()

        case GetEc2Property(name) =>
          SshInterpreter.getKey(name)

      }
    }
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
