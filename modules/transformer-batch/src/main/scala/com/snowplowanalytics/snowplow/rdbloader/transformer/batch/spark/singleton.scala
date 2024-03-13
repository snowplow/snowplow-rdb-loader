/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark

import cats.Id
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.show._
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverConfig
import com.snowplowanalytics.iglu.client.resolver.registries.JavaNetRegistryLookup.idLookupInstance
import com.snowplowanalytics.iglu.schemaddl.Properties
import com.snowplowanalytics.lrumap.CreateLruMap
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.eventsmanifest.{EventsManifest, EventsManifestConfig}
import com.snowplowanalytics.snowplow.rdbloader.common._
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.EventUtils.EventParser
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{EventUtils, PropertiesCache, PropertiesKey}
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.Config
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.Config.Output.BadSink
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows.{BadrowSink, KinesisSink, WiderowFileSink}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.SerializableConfiguration

/** Singletons needed for unserializable or stateful classes. */
object singleton {

  /** Singleton for Iglu's Resolver to maintain one Resolver per node. */
  object IgluSingleton {

    @volatile private var instance: Resolver[Id] = _

    /**
     * Retrieve or build an instance of Iglu's Resolver.
     *
     * @param igluConfig
     *   JSON representing the Iglu configuration
     */
    def get(resolverConfig: ResolverConfig): Resolver[Id] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = getIgluResolver(resolverConfig)
              .valueOr(e => throw new IllegalArgumentException(e))
          }
        }
      }
      instance
    }

    /**
     * Build an Iglu resolver from a JSON.
     *
     * @param igluConfig
     *   JSON representing the Iglu resolver
     * @return
     *   A Resolver or one or more error messages boxed in a Scalaz ValidationNel
     */
    private def getIgluResolver(resolverConfig: ResolverConfig): Either[String, Resolver[Id]] =
      Resolver.fromConfig[Id](resolverConfig).leftMap(_.show).value
  }

  /** Singleton for DuplicateStorage to maintain one per node. */
  object DuplicateStorageSingleton {

    @volatile private var instance: Option[EventsManifest] = _

    /**
     * Retrieve or build an instance of DuplicateStorage.
     *
     * @param dupStorageConfig
     *   configuration for DuplicateStorage
     */
    def get(dupStorageConfig: Option[EventsManifestConfig]): Option[EventsManifest] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = dupStorageConfig match {
              case Some(EventsManifestConfig.DynamoDb(_, "local", _, _, _)) =>
                Some(new InMemoryEventManifest)
              case Some(config) => EventsManifest.initStorage(config).fold(e => throw new IllegalArgumentException(e), _.some)
              case None => None
            }
          }
        }
      }
      instance
    }
  }

  object PropertiesCacheSingleton {
    @volatile private var instance: PropertiesCache[Id] = _

    def get(resolverConfig: ResolverConfig): PropertiesCache[Id] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = CreateLruMap[Id, PropertiesKey, Properties].create(resolverConfig.cacheSize)
          }
        }
      }
      instance
    }
  }

  object EventParserSingleton {
    @volatile private var instance: EventParser = _

    def get(config: Config, resolverConfig: ResolverConfig): EventParser = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            val atomicLengths =
              if (config.featureFlags.truncateAtomicFields) {
                EventUtils.getAtomicLengths(IgluSingleton.get(resolverConfig)).fold(err => throw err, identity)
              } else {
                Map.empty[String, Int]
              }
            instance = Event.parser(atomicLengths)
          }
        }
      }
      instance
    }
  }

  object BadrowSinkSingleton {
    @volatile private var instance: BadrowSink = _

    def get(
      config: Config,
      folderName: String,
      hadoopConfigBroadcasted: Broadcast[SerializableConfiguration]
    ): BadrowSink = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            val sink = config.output.bad match {
              case config: BadSink.Kinesis =>
                KinesisSink.createFrom(config)
              case BadSink.File =>
                WiderowFileSink.create(folderName, hadoopConfigBroadcasted.value.value, config.output.path, config.output.compression)
            }
            instance = sink
          }
        }
      }
      instance
    }
  }

}
