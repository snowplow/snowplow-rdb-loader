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

import com.snowplowanalytics.iglu.client.Resolver

import org.joda.time.DateTime

import com.snowplowanalytics.snowplow.scalatracker.Tracker

import com.snowplowanalytics.manifest.core.ManifestError

// This project
import LoaderA._
import LoaderError.LoaderLocalError
import config.CliConfig
import discovery.ManifestDiscovery
import utils.Common
import implementations._
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
  tracker: Option[Tracker],
  resolver: Resolver) extends Interpreter {

  private val interpreter = this

  /**
    * Successfully fetched JSONPaths
    * Key: "vendor/filename_1.json";
    * Value: "s3://my-jsonpaths/redshift/vendor/filename_1.json"
    */
  private val cache = collection.mutable.HashMap.empty[String, Option[S3.Key]]

  // dbConnection is Either because not required for log dump
  // lazy to wait before tunnel established
  private lazy val dbConnection = PgInterpreter.getConnection(cliConfig.target)

  lazy val manifest =
    ManifestInterpreter.initialize(cliConfig.target.processingManifest, cliConfig.configYaml.aws.s3.region, resolver)

  private val messages = collection.mutable.ListBuffer.empty[String]

  def log(message: String) = {
    val endMessage = s"RDB Loader [${DateTime.now()}]: $message"
    System.out.println(endMessage)
    messages.append(endMessage)
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


        case ManifestDiscover(application, predicate) =>
          for {
            optionalManifest <- manifest
            manifestClient <- optionalManifest match {
              case Some(manifestClient) =>
                log(s"Discovering through processing manifest [${manifestClient.primaryTable}]")
                manifestClient.asRight
              case None => LoaderLocalError("Processing Manifest is not configured").asLeft
            }
            result <- ManifestInterpreter.getUnprocessed(manifestClient, application, predicate) match {
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
            optionalManifest <- manifest
            manifestClient <- optionalManifest match {
              case Some (manifestClient) => manifestClient.asRight
              case None => LoaderLocalError("Processing Manifest is not configured").asLeft
            }
            process = ManifestInterpreter.process(interpreter, load)
            app = ManifestDiscovery.getLoaderApp(cliConfig.target.id)
            _ <- manifestClient.processItem(app, None, process)(item).leftMap {
              case ManifestError.ApplicationError(e, _, _) =>
                val message = Option(e.getMessage).getOrElse(e.toString)
                LoaderError.StorageTargetError(message)
              case e => LoaderError.fromManifestError(e)
            }
          } yield ()

        case ExecuteUpdate(query) =>
          if (query.startsWith("COPY ")) { log(query.split(" ").take(2).mkString(" ")) }

          val result = for {
            conn <- dbConnection
            res <- PgInterpreter.executeUpdate(conn)(query)
          } yield res
          result.asInstanceOf[Id[A]]
        case CopyViaStdin(files, query) =>
          for {
            conn <- dbConnection
            _ = log(s"Copying ${files.length} files via stdin")
            res <- PgInterpreter.copyViaStdin(conn, files, query)
          } yield res

        case ExecuteQuery(query, d) =>
          for {
            conn <- dbConnection
            res <- PgInterpreter.executeQuery(conn)(query)(d)
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
          dbConnection.foreach(c => c.close())
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
