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
package com.snowplowanalytics.snowplow
package storage.spark

// Iglu
import com.snowplowanalytics.iglu.client.Resolver
import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jsonschema.core.report.ProcessingMessage

// SCE
import enrich.common.{FatalEtlError, ValidatedNelMessage}

// This project
import utils.base64.base64ToJsonNode

/** Singletons needed for unserializable or stateful classes. */
object singleton {

  /** Singleton for Iglu's Resolver to maintain one Resolver per node. */
  object ResolverSingleton {

    @volatile private var instance: Resolver = _

    /**
     * Retrieve or build an instance of Iglu's Resolver.
     * @param igluConfig JSON representing the Iglu configuration
     */
    def get(igluConfig: String): Resolver = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = getIgluResolver(igluConfig)
              .valueOr(e => throw new FatalEtlError(e.toString))
          }
        }
      }
      instance
    }

    /**
     * Build an Iglu resolver from a JSON.
     * @param igluConfig JSON representing the Iglu resolver
     * @return A Resolver or one or more error messages boxed in a Scalaz ValidationNel
     */
    private[spark] def getIgluResolver(igluConfig: String): ValidatedNelMessage[Resolver] = {
      val json = base64ToJsonNode(igluConfig, "iglu")

      for {
        node <- json.toValidationNel[ProcessingMessage, JsonNode]
        resolver <- Resolver.parse(node)
      } yield resolver
    }
  }

  /** Singleton for DuplicateStorage to maintain one per node. */
  object DuplicateStorageSingleton {
    import DuplicateStorage._

    @volatile private var instance: Option[DuplicateStorage] = _

    /**
     * Retrieve or build an instance of DuplicateStorage.
     * @param dupStorageConfig configuration for DuplicateStorage
     */
    def get(dupStorageConfig: Option[DuplicateStorageConfig]): Option[DuplicateStorage] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = dupStorageConfig.map(initStorage) match {
              case Some(v) => v.fold(e => throw new FatalEtlError(e.toString), c => Some(c))
              case None => None
            }
          }
        }
      }
      instance
    }
  }
}