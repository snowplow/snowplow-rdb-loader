/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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

import scala.util.{Failure, Success}

import cats.data._
import cats.implicits._
import cats.effect._

import io.circe.Json

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.manifest.core.{Item, ManifestError, ProcessingManifest, Application}
import com.snowplowanalytics.manifest.dynamodb.DynamoDbManifest

import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget.ProcessingManifestConfig
import com.snowplowanalytics.snowplow.rdbloader.discovery.ManifestDiscovery
import com.snowplowanalytics.snowplow.rdbloader.interpreters.Interpreter

import scala.util.control.NonFatal

object ManifestInterpreter {

  type ManifestE[A] = EitherT[IO, ManifestError, A]
  type LoaderE[A] = EitherT[IO, LoaderError, A]

  def initialize(manifestConfig: Option[ProcessingManifestConfig],
                 emrRegion: String,
                 resolver: Client[ManifestE, Json]): Either[LoaderError, Option[DynamoDbManifest[ManifestE]]] = {
    try {
      manifestConfig.map { config =>
        val dynamodbClient = AmazonDynamoDBClientBuilder.standard().withRegion(emrRegion).build()
        DynamoDbManifest[ManifestE](dynamodbClient, config.amazonDynamoDb.tableName, resolver.resolver)
      }.asRight[LoaderError]
    } catch {
      case NonFatal(e) =>
        val error: LoaderError =
          LoaderError.LoaderLocalError(s"Cannot initialize DynamoDB client for processing manifest, ${e.toString}")
        error.asLeft[Option[DynamoDbManifest[ManifestE]]]
    }
  }

  def getUnprocessed(manifest: ProcessingManifest[ManifestE],
                     loader: Application,
                     shredder: Application,
                     predicate: Option[Item => Boolean])
                    (implicit S: Sync[ManifestE]): Either[LoaderError, List[Item]] = {
    manifest
      .getUnprocessed(manifest.query(None, Some(loader)), predicate.getOrElse(_ => true))
      .map(ManifestDiscovery.filterShredded(shredder))
      .leftMap(LoaderError.fromManifestError)
      .value
      .unsafeRunSync()
  }

  /**
    * Run load action through processor to actually perform it (if interpreter assumes it)
    * @param interpreter interpreter for database loading actions
    * @param load action to interpret, should contain only DB-interactions
    */
  def process(interpreter: Interpreter, load: LoaderAction[Unit]): ProcessingManifest.Process =
    (_: Item) => load.value.foldMap(interpreter.run) match {    // Item is ignored because we already parsed it
      case Left(e) => Failure(LoaderError.LoaderThrowable(e))   // during discovery step
      case Right(_) => Success(None)
    }
}
