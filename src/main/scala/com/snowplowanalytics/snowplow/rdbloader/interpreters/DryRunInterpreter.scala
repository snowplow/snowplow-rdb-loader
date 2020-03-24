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

import java.nio.file._

import scala.collection.mutable.ListBuffer

import cats._
import cats.implicits._

import io.circe.Json

import com.amazonaws.services.s3.AmazonS3

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.snowplow.scalatracker.Tracker

// This project
import common._

import LoaderA._
import config.CliConfig
import loaders.Common.SqlString
import discovery.DiscoveryFailure
import implementations.{S3Interpreter, TrackerInterpreter}


/**
  * Interpreter performs all actual side-effecting work,
  * interpreting `Action` at the end-of-the-world.
  * It contains and handles configuration, connections and mutable state,
  * all real-world interactions, except argument parsing
  */
class DryRunInterpreter private[interpreters](cliConfig: CliConfig,
                                              amazonS3: AmazonS3,
                                              tracker: Option[Tracker[Id]],
                                              igluClient: Client[Id, Json]) extends Interpreter {

  private val logQueries = ListBuffer.empty[SqlString]
  private val logCopyFiles = ListBuffer.empty[Path]
  private val logMessages = ListBuffer.empty[String]
  private var sleepTime = 0L

  private val interpreter = this

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

        case ExecuteUpdate(query) =>
          logQueries.append(query)
          0L.asRight[LoaderError]
        case CopyViaStdin(files, _) =>
          // Will never work while `DownloadData` is noop
          logCopyFiles.appendAll(files)
          0L.asRight[LoaderError]

        case ExecuteQuery(_, _) =>
          None.asRight    // All used decoders return something with Option

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
        case Track(log) =>
          println(log.toString)
        case Dump(key) =>
          val dryRunResult = "Dry-run action: \n" + getDryRunLogs
          TrackerInterpreter.dumpStdout(amazonS3, key, dryRunResult)
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

        case GetSchemas(vendor, name, model) =>
          Flattening.getOrdered(igluClient.resolver, vendor, name, model).leftMap { resolutionError =>
            val message = s"Cannot get schemas for iglu:$vendor/$name/jsonschema/$model-*-*\n$resolutionError"
            LoaderError.DiscoveryError(DiscoveryFailure.IgluError(message))
          }.value
      }
    }
  }
}
