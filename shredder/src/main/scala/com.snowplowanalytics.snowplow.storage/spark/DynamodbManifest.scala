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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.manifest.core.{Application, ManifestError }
import com.snowplowanalytics.manifest.core.ProcessingManifest._
import com.snowplowanalytics.manifest.dynamodb.DynamoDbManifest

import com.snowplowanalytics.snowplow.rdbloader.generated.ProjectMetadata

object DynamodbManifest {

  type ManifestFailure[A] = Either[ManifestError, A]

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

  /** Create a DynamoDB-backed Processing Manifest client */
  def initialize(tableName: String, resolver: Resolver): DynamoDbManifest[ManifestFailure] = {
    val client = AmazonDynamoDBClientBuilder.standard().build()
    DynamoDbManifest[ManifestFailure](client, tableName, resolver)
  }
}
