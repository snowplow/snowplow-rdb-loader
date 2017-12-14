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

import java.nio.file._

import scala.collection.mutable.ListBuffer

import cats._
import cats.implicits._

import com.amazonaws.services.s3.AmazonS3

import com.snowplowanalytics.snowplow.scalatracker.Tracker

// This project
import config.CliConfig
import LoaderA._
import loaders.Common.SqlString
import implementations.{S3Interpreter, TrackerInterpreter}

/**
  * Interpreter performs all actual side-effecting work,
  * interpreting `Action` at the end-of-the-world.
  * It contains and handles configuration, connections and mutable state,
  * all real-world interactions, except argument parsing
  */
class DryRunInterpreter private[interpreters](
    cliConfig: CliConfig,
    amazonS3: AmazonS3,
    tracker: Option[Tracker]) extends Interpreter {

  private val logQueries = ListBuffer.empty[SqlString]
  private val logCopyFiles = ListBuffer.empty[Path]
  private val logMessages = ListBuffer.empty[String]
  private var sleepTime = 0L

  /**
    * Successfully fetched JSONPaths
    * Key: "vendor/filename_1.json";
    * Value: "s3://my-jsonpaths/redshift/vendor/filename_1.json"
    */
  private val cache = collection.mutable.HashMap.empty[String, Option[S3.Key]]

  def getDryRunLogs: String = {
    val sleep = s"Consistency check sleep time: $sleepTime\n"
    val queries =
      if (logQueries.nonEmpty) "Performed SQL Queries:\n" + logQueries.mkString("\n")
      else "No SQL queries performed"
    val messages =
      if (logMessages.nonEmpty) "Debug messages:\n" + logMessages.mkString("\n")
      else ""
    val files =
      if (logCopyFiles.nonEmpty) "Files loaded via stdin:\n" + logCopyFiles.mkString("\n")
      else ""

    List(sleep, queries, messages, files).mkString("\n")
  }

  def run: LoaderA ~> Id = new (LoaderA ~> Id) {

    def apply[A](effect: LoaderA[A]): Id[A] = {
      effect match {
        case ListS3(folder) =>
          S3Interpreter.list(amazonS3, folder).map(summaries => summaries.map(S3.getKey))
        case KeyExists(key) =>
          S3Interpreter.keyExists(amazonS3, key)
        case DownloadData(source, dest) =>
          logMessages.append(s"Downloading data from [$source] to [$dest]")
          List.empty[Path].asRight[LoaderError]

        case ExecuteQuery(query) =>
          logQueries.append(query)
          0L.asRight[LoaderError]
        case CopyViaStdin(files, _) =>
          // Will never work while `DownloadData` is noop
          logCopyFiles.appendAll(files)
          0L.asRight[LoaderError]

        case CreateTmpDir =>
          logMessages.append("Created temporary directory")
          Paths.get("tmp").asRight

        case DeleteDir(path) =>
          logMessages.append(s"Deleted temporary directory [${path.toString}]").asRight


        case Print(message) =>
          println(message)
        case Sleep(timeout) =>
          sleepTime = sleepTime + timeout
          Thread.sleep(timeout)
        case Track(_) =>
          ()
        case Dump(key, result) =>
          val actionResult = result.toString + "\n"
          val dryRunResult = "Dry-run action: \n" + getDryRunLogs
          TrackerInterpreter.dumpStdout(amazonS3, key, actionResult + dryRunResult)
        case Exit(loadResult, dumpResult) =>
          println("Dry-run action: \n" + getDryRunLogs)
          TrackerInterpreter.exit(loadResult, dumpResult)

        case Get(key: String) =>
          cache.get(key)
        case Put(key: String, value: Option[S3.Key]) =>
          val _ = cache.put(key, value)
          ()

        case EstablishTunnel(tunnel) =>
          Right(logMessages.append(s"Established imaginary SSH tunnel to [${tunnel.config.bastion.host}:${tunnel.config.bastion.port}]"))
        case CloseTunnel() =>
          Right(logMessages.append(s"Closed imaginary SSH tunnel"))

        case GetEc2Property(name) =>
          logMessages.append(s"Fetched imaginary EC2 [$name] property")
          Right(name + " key")

      }
    }
  }
}

