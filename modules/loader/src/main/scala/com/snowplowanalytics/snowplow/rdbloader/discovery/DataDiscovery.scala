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
package com.snowplowanalytics.snowplow.rdbloader.discovery

import cats._
import cats.data._
import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.{DiscoveryStep, LoaderAction, LoaderError}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.ShreddingComplete
import com.snowplowanalytics.snowplow.rdbloader.common.{AtomicSubpathPattern, S3}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{AWS, Cache}

/**
  * Result of data discovery in shredded.good folder
  * It still exists mostly to fulfill legacy batch-discovery behavior,
  * once Loader entirely switched to RT architecture we can replace it with
  * `ShreddingComplete` message
  * @param base shred run folder full path
  * @param shreddedTypes list of shredded types in this directory
  */
case class DataDiscovery(base: S3.Folder, shreddedTypes: List[ShreddedType]) {
  /** ETL id */
  def runId: String = base.split("/").last

  /** `atomic-events` directory full path */
  def atomicEvents: S3.Folder =
    S3.Folder.append(base, "atomic-events")

  def show: String = {
    val shreddedTypesList = shreddedTypes.map(x => s"  * ${x.show}").mkString("\n")
    val shredded = if (shreddedTypes.isEmpty) "without shredded types" else s"with following shredded types:\n$shreddedTypesList"
    s"$runId $shredded"
  }
}

/**
 * This module provides data-discovery mechanism for "atomic" events only
 * and for "full" discovery (atomic and shredded types)
 * Primary methods return lists of `DataDiscovery` results, where
 * each `DataDiscovery` represents particular `run=*` folder in shredded.good
 *
 * It lists all keys in run id folder, parse each one as atomic-events key or
 * shredded type key and then groups by run ids
 */
object DataDiscovery {

  type Discovered = List[DataDiscovery]

  /** Amount of times consistency check will be performed */
  val ConsistencyChecks = 5

  /** Legacy */
  sealed trait DiscoveryTarget extends Product with Serializable
  object DiscoveryTarget {
    final case class Global(folder: S3.Folder) extends DiscoveryTarget
  }

  /**
   * Convert `ShreddingComplete` message coming from shredder into
   * actionable `DataDiscovery`
   * @param region AWS region to discover JSONPaths
   * @param jsonpathAssets optional bucket with custo mJSONPaths
   * @param message payload coming from shredder
   * @tparam F effect type to perform AWS interactions
   */
  def fromLoaderMessage[F[_]: Monad: Cache: AWS](region: String,
                                                 jsonpathAssets: Option[S3.Folder],
                                                 message: ShreddingComplete): LoaderAction[F, DataDiscovery] = {
    val types = message
      .types
      .traverse[F, DiscoveryStep[ShreddedType]] { shreddedType =>
        ShreddedType.fromCommon[F](message.base, message.processor.version, region, jsonpathAssets, shreddedType)
      }
      .map { steps => LoaderError.flattenValidated(steps.traverse(_.toValidatedNel)) }
    LoaderAction[F, List[ShreddedType]](types).map { types =>
      DataDiscovery(message.base, types)
    }
  }

  /**
   * List whole directory excluding special files
   */
  def listGoodBucket[F[_]: Functor: AWS](folder: S3.Folder): LoaderAction[F, List[S3.BlobObject]] =
    EitherT(AWS[F].listS3(folder)).map(_.filterNot(k => isSpecial(k.key)))

  // Common

  def isAtomic(key: S3.Key): Boolean = key match {
    case AtomicSubpathPattern(_, _, _) => true
    case _ => false
  }

  def isSpecial(key: S3.Key): Boolean = key.contains("$") || key.contains("_SUCCESS")
}
