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
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverConfig
import com.snowplowanalytics.iglu.schemaddl.Properties
import com.snowplowanalytics.lrumap.CreateLruMap
import com.snowplowanalytics.snowplow.eventsmanifest.{EventsManifest, EventsManifestConfig}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{PropertiesCache, PropertiesKey}
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.{Cloud, Config}

import java.io.{File, PrintWriter}
import java.util.UUID
import scala.collection.mutable
import scala.util.Try

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

  object ParquetBadrowsAccumulator {

    Runtime.getRuntime
      .addShutdownHook(new Thread(() => flushPending()))

    @volatile private var config: Config = _

    private val maxPerFile = Try(System.getProperty("experimental.parquet.badrows.maxPerFile").toInt).getOrElse(1000)
    private val badrowsMap: mutable.Map[String, mutable.ListBuffer[String]] = mutable.HashMap.empty

    def addBadrow(
      badRow: String,
      config: Config,
      folderName: String
    ): Unit =
      synchronized {
        this.config = config

        badrowsMap.get(folderName) match {
          case Some(folderBuffer) =>
            folderBuffer += badRow
            if (folderBuffer.size >= maxPerFile) {
              flush(folderName, folderBuffer)
            }
          case None =>
            val tuple = (folderName, mutable.ListBuffer(badRow))
            badrowsMap += tuple
        }
        ()
      }

    private def flushPending(): Unit =
      synchronized {
        if (badrowsMap.nonEmpty) {
          badrowsMap
            .filter(_._2.nonEmpty)
            .foreach { case (folderName, buffer) =>
              flush(folderName, buffer)
            }
        }
      }

    private def flush(folderName: String, buffer: mutable.ListBuffer[String]): Unit = {
      val badrowsContent = buffer.mkString("\n")
      val fileName = s"part-${UUID.randomUUID()}"

      if (this.config.output.path.toString.startsWith("file")) {
        flushToFilesystem(badrowsContent, fileName)
      } else {
        flushToS3(folderName, badrowsContent, fileName)
      }

      buffer.clear()
    }

    private def flushToS3(
      folderName: String,
      badrowsContent: String,
      fileName: String
    ) = {
      val client = Cloud.createS3Client(this.config.output.region)
      val folder = BlobStorage.Folder.coerce(this.config.output.path.toString).append(folderName).append("output=bad")

      val (bucket, key) = BlobStorage.splitKey(folder.withKey(fileName))
      Cloud.putToS3(client, bucket, key, badrowsContent)
    }

    private def flushToFilesystem(badrowsContent: String, fileName: String) = {
      val outFolder: File = new File(this.config.output.path.getPath + "/output=bad")
      outFolder.mkdirs()

      val file = new File(outFolder, fileName)
      val writer = new PrintWriter(file)

      writer.write(badrowsContent)
      writer.close()
    }
  }
}
