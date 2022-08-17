/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark

import cats.Id
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.show._
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.schemaddl.Properties
import com.snowplowanalytics.lrumap.CreateLruMap
import com.snowplowanalytics.snowplow.eventsmanifest.{DynamoDbManifest, EventsManifest, EventsManifestConfig}
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{LookupProperties, PropertiesKey}
import io.circe.Json

/** Singletons needed for unserializable or stateful classes. */
object singleton {

  /** Singleton for Iglu's Resolver to maintain one Resolver per node. */
  object IgluSingleton {

    @volatile private var instance: Client[Id, Json] = _

    /**
      * Retrieve or build an instance of Iglu's Resolver.
      *
      * @param igluConfig JSON representing the Iglu configuration
      */
    def get(igluConfig: Json): Client[Id, Json] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = getIgluClient(igluConfig)
              .valueOr(e => throw new IllegalArgumentException(e))
          }
        }
      }
      instance
    }

    /**
      * Build an Iglu resolver from a JSON.
      *
      * @param igluConfig JSON representing the Iglu resolver
      * @return A Resolver or one or more error messages boxed in a Scalaz ValidationNel
      */
    private[spark] def getIgluClient(igluConfig: Json): Either[String, Client[Id, Json]] =
      Client.parseDefault[Id](igluConfig).leftMap(_.show).value
  }

  /** Singleton for DuplicateStorage to maintain one per node. */
  object DuplicateStorageSingleton {

    @volatile private var instance: Option[EventsManifest] = _

    /**
      * Retrieve or build an instance of DuplicateStorage.
      *
      * @param dupStorageConfig configuration for DuplicateStorage
      */
    def get(dupStorageConfig: Option[EventsManifestConfig]): Option[EventsManifest] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = dupStorageConfig match {
              case Some(EventsManifestConfig.DynamoDb(None, "local", None, region, table)) =>
                val client = AmazonDynamoDBClientBuilder
                  .standard()
                  .withEndpointConfiguration(new EndpointConfiguration("http://localhost:8000", region))
                  .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("local", "local")))
                  .build()
                Some(new DynamoDbManifest(client, table))
              case Some(config) => EventsManifest.initStorage(config).fold(e => throw new IllegalArgumentException(e), _.some)
              case None => None
            }
          }
        }
      }
      instance
    }
  }

  object PropertiesLookupSingleton {
    @volatile private var instance: LookupProperties[Id] = _

    def get: LookupProperties[Id] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = CreateLruMap[Id, PropertiesKey, Properties].create(100)
          }
        }
      }
      instance
    }
  }
}
