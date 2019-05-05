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
import java.time.Instant
import java.util.UUID

import scala.collection.mutable.ListBuffer

import cats._
import cats.data._
import cats.implicits._
import cats.effect.IO

import io.circe.Json

import com.amazonaws.services.s3.AmazonS3

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.snowplow.scalatracker.Tracker

import com.snowplowanalytics.manifest.core.{ManifestError, ProcessingManifest, LockHandler}

// This project
import config.CliConfig
import LoaderA._
import LoaderError.LoaderLocalError
import Interpreter.runIO
import loaders.Common.SqlString
import discovery.{ ManifestDiscovery, DiscoveryFailure }
import implementations.{S3Interpreter, TrackerInterpreter, ManifestInterpreter}
import implementations.ManifestInterpreter.ManifestE
import com.snowplowanalytics.snowplow.rdbloader.common.Flattening.getOrdered


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

  lazy val manifest =
    ManifestInterpreter.initialize(cliConfig.target.processingManifest, cliConfig.configYaml.aws.s3.region, utils.Common.DefaultClient) match {
      case Right(Some(m)) => m.asRight
      case Right(None) => LoaderLocalError("Processing Manifest is not configured").asLeft
      case Left(error) => error.asLeft
    }

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

        case ManifestDiscover(loader, shredder, predicate) =>
          for {
            manifestClient <- manifest
            result <- ManifestInterpreter.getUnprocessed(manifestClient, loader, shredder, predicate)
          } yield result
        case ManifestProcess(item, load) =>
          for {
            manifestClient <- manifest
            process = ManifestInterpreter.process(interpreter, load)
            app = ManifestDiscovery.getLoaderApp(cliConfig.target.id)
            dummyHandler = DryRunInterpreter.dummyLockHandler(LockHandler.Default(manifestClient))
            processItem = ProcessingManifest.processItemWithHandler(dummyHandler) _
            _ <- runIO(processItem(app, None, process)(item).leftMap(LoaderError.fromManifestError).value)
          } yield ()

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
          getOrdered(igluClient.resolver, vendor, name, model).leftMap { resolutionError =>
            val message = s"Cannot get schemas for iglu:$vendor/$name/jsonschema/$model-*-*\n$resolutionError"
            LoaderError.DiscoveryError(DiscoveryFailure.IgluError(message))
          }.value
      }
    }
  }
}

object DryRunInterpreter {

  type LockHandlerF = LockHandler[ManifestE]

  /** Update LockHandler to not perform any real actions and never fail */
  def dummyLockHandler(lockHandler: LockHandlerF): LockHandlerF = {
    lockHandler
      .copy(release = (_, _, _, _) => {
        println("release")
        EitherT.pure[IO, ManifestError]((UUID.randomUUID(), Instant.now()))
      })
      .copy(acquire = (_, _, _) => {
        println("acquired")
        EitherT.pure[IO, ManifestError]((UUID.randomUUID(), Instant.now()))
      })
  }
}