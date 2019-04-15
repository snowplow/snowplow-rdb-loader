/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.storage.spark

import io.circe.Json

import cats.implicits._
import cats.effect.Clock

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.{Registry, RegistryError, RegistryLookup}
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData, SchemaList}

import com.snowplowanalytics.manifest.ItemId
import com.snowplowanalytics.manifest.core.{Application, ManifestError}
import com.snowplowanalytics.manifest.dynamodb.DynamoDbManifest

import com.snowplowanalytics.snowplow.rdbloader.generated.ProjectMetadata

import scala.concurrent.duration.TimeUnit

object DynamodbManifest {

  type ManifestFailure[A] = Either[ManifestError, A]

  implicit val manifestFailureCloeck: Clock[ManifestFailure] = new Clock[ManifestFailure] {
    override def realTime(unit: TimeUnit): ManifestFailure[Long] = Right(???)

    override def monotonic(unit: TimeUnit): ManifestFailure[Long] = Right(???)
  }

  implicit val manifestFailureLookup: RegistryLookup[ManifestFailure] = new RegistryLookup[ManifestFailure] {
    def lookup(repositoryRef: Registry, schemaKey: SchemaKey): ManifestFailure[Either[RegistryError, Json]] = ???
    def list(registry: Registry, vendor: String, name: String): ManifestFailure[Option[SchemaList]] = ???
  }

  case class ShredderManifest(manifest: DynamoDbManifest[ManifestFailure], itemId: ItemId)

  /** Application Constant */
  val ShredJobApplication = Application(ProjectMetadata.name, ProjectMetadata.version)

  val ProcessedPayloadKey: SchemaKey =
    SchemaKey("com.snowplowanalytics.snowplow.storage.rdbshredder", "processed_payload", "jsonschema", SchemaVer.Full(1, 0, 0))

  /** Create JSON Payload for PROCESSED record */
  def processedPayload(shreddedTypes: Set[String]): Option[SelfDescribingData[Json]] = {
    val data = Json.obj("shreddedTypes" -> Json.arr(shreddedTypes.map(Json.fromString).toList: _*))
    val payload = SelfDescribingData(ProcessedPayloadKey, data)
    Some(payload)
  }

  /** Coerces run-folder paths to a normal form ("s3:" instead of "s3a"), trailing slash */
  def normalizeItemId(s3path: String): String = {
    def normalize(tail: List[String]): String =
      "s3:/" ++ tail.mkString("/") ++ "/"

    s3path.split("/").toList match {
      case "s3a:" :: tail => normalize(tail)
      case "s3n:" :: tail => normalize(tail)
      case "s3:" :: tail => normalize(tail)
      case protocol :: _ =>
        throw new IllegalArgumentException(s"Unknown protocol [$protocol] in [$s3path]. Only s3a, s3n, s3 are allowed")
      case Nil =>
        throw new IllegalArgumentException(s"Invalid item id [$s3path]. Impossible to add to processing manifest")
    }
  }

  /** Create a DynamoDB-backed Processing Manifest client */
  def initialize[F[_]](tableName: String, igluClient: Client[ManifestFailure, Json]): DynamoDbManifest[ManifestFailure] = {
    val client = AmazonDynamoDBClientBuilder.standard().build()
    DynamoDbManifest[ManifestFailure](client, tableName, igluClient.resolver)
  }
}
