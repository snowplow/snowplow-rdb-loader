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

import scala.concurrent.duration._

import cats._
import cats.data._
import cats.implicits._

import cats.effect.Timer

import com.snowplowanalytics.snowplow.rdbloader.{DiscoveryStep, LoaderAction, LoaderError, sequenceInF}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.ShreddingComplete
import com.snowplowanalytics.snowplow.rdbloader.common.{AtomicSubpathPattern, Semver, S3}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, AWS, Cache}

/**
  * Result of data discovery in shredded.good folder
  * It still exists mostly to fulfill legacy batch-discovery behavior,
  * once Loader entirely switched to RT architecture we can replace it with
  * `ShreddingComplete` message
  * @param base shred run folder full path
  * @param atomicCardinality amount of keys in atomic-events directory if known
  * @param shreddedTypes list of shredded types in this directory
  */
case class DataDiscovery(
    base: S3.Folder,
    atomicCardinality: Option[Long],
    atomicSize: Option[Long],
    shreddedTypes: List[ShreddedType]) {
  /** ETL id */
  def runId: String = base.split("/").last

  /** `atomic-events` directory full path */
  def atomicEvents: S3.Folder =
    S3.Folder.append(base, "atomic-events")

  /**
   * Time in ms for run folder to setup eventual consistency,
   * based on amount of atomic and shredded files
   */
  def consistencyTimeout: Long = atomicCardinality match {
    case Some(cardinality) =>
      ((cardinality * 0.1 * shreddedTypes.length).toLong + 5L) * 1000
    case None => 0L
  }

  def show: String = {
    val atomic = atomicCardinality.map(_.toString).getOrElse("unknown amount of")
    val shreddedTypesList = shreddedTypes.map(x => s"  * ${x.show}").mkString("\n")
    val shredded = if (shreddedTypes.isEmpty) "without shredded types" else s"with following shredded types:\n$shreddedTypesList"
    val size = atomicSize.map(s => s" (${(s / 1000000).toString} Mb) ").getOrElse(" ")
    s"$runId with $atomic atomic files${size}and $shredded"
  }

  /** Check if atomic events possibly contains only empty files */
  def possiblyEmpty: Boolean = (atomicCardinality, atomicSize) match {
    case (Some(c), Some(s)) => s / c <= 20  // 20 bytes is size of empty gzipped file
    case _ => false                         // Impossible to deduce
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
      DataDiscovery(message.base, None, None, types)
    }
  }

  /**
   * Discover list of shred run folders, each containing
   * exactly one `atomic-events` folder and zero or more `ShreddedType`s
   *
   * Action fails if:
   * + no atomic-events folder found in *any* shred run folder
   * + files with unknown path were found in *any* shred run folder
   *
   * @param target either shredded good or specific run folder
   * @param shredJob shred job version to check path pattern
   * @param region AWS region for S3 buckets
   * @param assets optional JSONPath assets S3 bucket
   * @return list (probably empty, but usually with single element) of discover results
   *         (atomic events and shredded types)
   */
  def discover[F[_]: Monad: Cache: Logging: AWS](target: DiscoveryTarget,
                                                 shredJob: Semver,
                                                 region: String,
                                                 assets: Option[S3.Folder]): LoaderAction[F, Discovered] = {
    def group(validatedDataKeys: LoaderAction[F, ValidatedDataKeys[F]]): LoaderAction[F, Discovered] =
      for {
        keys <- validatedDataKeys
        discovery <- groupKeysFull[F](keys)
      } yield discovery

    target match {
      case DiscoveryTarget.Global(folder) =>
        val keys: LoaderAction[F, ValidatedDataKeys[F]] =
          listGoodBucket[F](folder).map(transformKeys[F](shredJob, region, assets))
        group(keys)
    }
  }

  /**
   * List whole directory excluding special files
   */
  def listGoodBucket[F[_]: Functor: AWS](folder: S3.Folder): LoaderAction[F, List[S3.BlobObject]] =
    EitherT(AWS[F].listS3(folder)).map(_.filterNot(k => isSpecial(k.key)))

  // Full discovery

  /**
   * Group list of keys into list (usually single-element) of `DataDiscovery`
   *
   * @param validatedDataKeys IO-action producing validated list of `FinalDataKey`
   * @return IO-action producing list of
   */
  def groupKeysFull[F[_]: Applicative](validatedDataKeys: ValidatedDataKeys[F]): LoaderAction[F, Discovered] = {
    def group(dataKeys: List[DataKeyFinal]): ValidatedNel[DiscoveryFailure, Discovered] =
      dataKeys.groupBy(_.base).toList.reverse.traverse(validateFolderFull)

    // Transform into Either with non-empty list of errors
    val result: F[Either[LoaderError, Discovered]] =
      validatedDataKeys.map { keys =>
        keys.andThen(group) match {
          case Validated.Valid(discovery) =>
            discovery.asRight
          case Validated.Invalid(failures) =>
            val aggregated = DiscoveryFailure.aggregateDiscoveryFailures(failures).distinct
            LoaderError.DiscoveryError(aggregated).asLeft
        }
      }
    EitherT(result)
  }

  /**
   * Check that S3 folder contains at least one atomic-events file
   * And split `FinalDataKey`s into "atomic" and "shredded"
   */
  def validateFolderFull(groupOfKeys: (S3.Folder, List[DataKeyFinal])): ValidatedNel[DiscoveryFailure, DataDiscovery] = {
    val empty = (List.empty[AtomicDataKey], List.empty[ShreddedDataKeyFinal])
    val (base, dataKeys) = groupOfKeys
    val (atomicKeys, shreddedKeys) = dataKeys.foldLeft(empty) { case ((atomic, shredded), key) =>
      key match {
        case atomicKey: AtomicDataKey => (atomicKey :: atomic, shredded)
        case shreddedType: ShreddedDataKeyFinal => (atomic, shreddedType :: shredded)
      }
    }

    if (atomicKeys.nonEmpty) {
      val shreddedData = shreddedKeys.map(_.info).distinct
      val size = Some(atomicKeys.foldMap(_.size))
      DataDiscovery(base, Some(atomicKeys.length.toLong), size, shreddedData).validNel
    } else {
      DiscoveryFailure.AtomicDiscoveryFailure(base).invalidNel
    }
  }

  /**
   * Transform list of S3 keys into list of `DataKeyFinal` for `DataDiscovery`
   */
  private def transformKeys[F[_]: Monad: Cache: AWS](shredJob: Semver,
                                                     region: String,
                                                     assets: Option[S3.Folder])
                                                    (keys: List[S3.BlobObject]): ValidatedDataKeys[F] = {
    // Intermediate keys are keys that passed one check and not yet passed another
    val intermediateDataKeys = keys.map(parseDataKey(shredJob, _))
    // Final keys passed all checks, e.g. JSONPaths for shredded data were fetched
    val finalDataKeys = intermediateDataKeys.traverse(transformDataKey[F](_, region, assets))
    sequenceInF(finalDataKeys, identity[ValidatedNel[DiscoveryFailure, List[DataKeyFinal]]])
  }

  /**
   * Transform intermediate `DataKey` into `ReadyDataKey` by finding JSONPath file
   * for each shredded type. Used to aggregate "invalid path" errors (produced by
   * `parseDataKey`) with "not found JSONPath"
   * Atomic data will be returned as-is
   *
   * @param dataKey either successful or failed data key
   * @param region AWS region for S3 buckets
   * @param assets optional JSONPath assets S3 bucket
   * @return `Action` containing `Validation` - as on next step we can aggregate errors
   */
  private def transformDataKey[F[_]: Monad: Cache: AWS](
      dataKey: DiscoveryStep[DataKeyIntermediate],
      region: String,
      assets: Option[S3.Folder]
  ): F[ValidatedNel[DiscoveryFailure, DataKeyFinal]] = {
    dataKey match {
      case Right(ShreddedDataKeyIntermediate(fullPath, info)) =>
        val jsonpathAction: EitherT[F, DiscoveryFailure, S3.Key] =
          EitherT(ShreddedType.discoverJsonPath[F](region, assets, info))
        val discoveryAction = jsonpathAction.map { jsonpath =>
          ShreddedDataKeyFinal(fullPath, ShreddedType.Json(info, jsonpath))
        }
        discoveryAction.value.map(_.toValidatedNel)
      case Right(key @ ShreddedDataKeyTabular(_, _)) =>
        Monad[F].pure(key.toFinal.validNel[DiscoveryFailure])
      case Right(AtomicDataKey(fullPath, size)) =>
          Monad[F].pure(AtomicDataKey(fullPath, size).validNel[DiscoveryFailure])
      case Left(failure) =>
          Monad[F].pure(failure.invalidNel)
    }
  }

  /**
   * Parse S3 key into valid atomic-events path or shredded type
   * This function will short-circuit whole discover process if found
   * any invalid S3 key - not atomic events file neither shredded type file
   *
   * @param shredJob shred job version to check path pattern
   * @param blobObject particular S3 key in shredded good folder
   */
  private def parseDataKey(shredJob: Semver, blobObject: S3.BlobObject): Either[DiscoveryFailure, DataKeyIntermediate] = {
    S3.getAtomicPath(blobObject.key) match {
      case Some(_) => AtomicDataKey(blobObject.key, blobObject.size).asRight
      case None =>
        ShreddedType.transformPath(blobObject.key, shredJob) match {
          case Right((false, info)) =>
            ShreddedDataKeyIntermediate(blobObject.key, info).asRight
          case Right((true, info)) =>
            ShreddedDataKeyTabular(blobObject.key, info).asRight
          case Left(e) => e.asLeft
        }
    }
  }

  // Common

  def isAtomic(key: S3.Key): Boolean = key match {
    case AtomicSubpathPattern(_, _, _) => true
    case _ => false
  }

  def isSpecial(key: S3.Key): Boolean = key.contains("$") || key.contains("_SUCCESS")

  // Consistency

  /**
   * Wait until S3 list result becomes consistent
   * Waits some time after initial request, depending on cardinality of discovered folders
   * then does identical request, and compares results
   * If results differ - wait and request again. Repeat 5 times until requests identical
   *
   * @param originalAction data-discovery action
   * @return result of same request, but with more guarantees to be consistent
   */
  def checkConsistency[F[_]: Monad: Timer: Logging](originalAction: LoaderAction[F, Discovered]): LoaderAction[F, Discovered] = {
    def check(checkAttempt: Int, last: Option[Either[LoaderError, Discovered]]): F[Either[LoaderError, Discovered]] = {
      val action = last.map(Monad[F].pure).getOrElse(originalAction.value)

      for {
        original <- action
        _        <- sleepConsistency[F](original)
        control  <- originalAction.value
        result   <- retry(original, control, checkAttempt + 1)
      } yield result
    }

    def retry(original: Either[LoaderError, Discovered], control: Either[LoaderError, Discovered], attempt: Int): F[Either[LoaderError, Discovered]] = {
      (original, control) match {
        case _ if attempt >= ConsistencyChecks =>
          for {
            _ <- Logging[F].print(s"Consistency check did not pass after $ConsistencyChecks attempts")
            discovered <- Monad[F].pure(control.orElse(original))
          } yield discovered
        case (Right(o), Right(c)) if o.sortBy(_.base.toString) == c.sortBy(_.base.toString) =>
          val found = o.map(x => s"+ ${x.show}").mkString("\n")
          val message = if (found.isEmpty) "No run ids discovered" else s"Following run ids found:\n$found"
          for {
            _ <- Logging[F].print(s"Consistency check passed after ${attempt - 1} attempt. " ++ message)
            discovered <- Monad[F].pure(original)
          } yield discovered
        case (Right(o), Right(c)) =>
          val message = if (attempt == ConsistencyChecks - 1)
            s"Difference:\n ${discoveryDiff(o, c).map(m => s"+ $m").mkString("\n")}"
          else ""

          for {
            _ <- Logging[F].print(s"Consistency check failed. $message")
            next <- check(attempt, Some(control))
          } yield next
        case _ =>
          for {
            _ <- Logging[F].print(s"Consistency check failed. Making another attempt")
            next <- check(attempt, None)
          } yield next
      }
    }

    EitherT[F, LoaderError, Discovered](check(1, None))
  }

  def discoveryDiff(original: Discovered, control: Discovered): List[String] = {
    original.flatMap { o =>
      control.find(_.base == o.base) match {
        case None => List(s"Folder ${o.base} was not found in control check (probably ghost-folder)")
        case Some(c) => discoveryDiff(o, c)
      }
    }
  }

  /** Get difference-message between two checks. Assuming they have same base */
  private def discoveryDiff(o: DataDiscovery, c: DataDiscovery) = {
    (if (o.atomicCardinality != c.atomicCardinality) List("Different cardinality of atomic files") else Nil) ++
      o.shreddedTypes.diff(c.shreddedTypes).map { d => s"${d.show} exists in first check and misses in control" } ++
      c.shreddedTypes.diff(o.shreddedTypes).map { d => s"${d.show} exists in control check and misses in first" }
  }

  /**
   * Aggregates wait time for all discovered folders or wait 10 sec in case action failed
   */
  private def sleepConsistency[F[_]: Timer](result: Either[LoaderError, Discovered]): F[Unit] = {
    val timeoutMs = result match {
      case Right(list) =>
        list.map(_.consistencyTimeout).foldLeft(10000L)(_ + _)
      case Left(_) => 10000L
    }

    Timer[F].sleep(timeoutMs.millis)
  }


  // Temporary "type tags", representing validated "atomic" or "shredded" S3 keys

  /**
   * S3 key, representing either atomic events file or shredded type file
   * It is intermediate because shredded type doesn't contain its JSONPath yet
   */
  private sealed trait DataKeyIntermediate

  /**
   * S3 key, representing either atomic events file or shredded type file
   * It is final because shredded type proved to have JSONPath
   */
  private sealed trait DataKeyFinal {
    def base: S3.Folder
    def key: S3.Key
  }

  /**
   * S3 key, representing atomic events file
   * It can be used as both "intermediate" and "final" because atomic-events
   * don't need any more validations, except path
   */
  private case class AtomicDataKey(key: S3.Key, size: Long) extends DataKeyIntermediate with DataKeyFinal {
    def base: S3.Folder = {
      val atomicEvents = S3.Key.getParent(key)
      S3.Folder.getParent(atomicEvents)
    }
  }

  /**
   * S3 key, representing intermediate shredded type file
   * It is intermediate because shredded type doesn't contain its JSONPath yet
   */
  private case class ShreddedDataKeyIntermediate(key: S3.Key, info: ShreddedType.Info) extends DataKeyIntermediate

  /** Shredded key that doesn't need a JSONPath file and can be mapped to final */
  private case class ShreddedDataKeyTabular(key: S3.Key, info: ShreddedType.Info) extends DataKeyIntermediate {
    def toFinal: ShreddedDataKeyFinal =
      ShreddedDataKeyFinal(key, ShreddedType.Tabular(info))
  }

  /**
   * S3 key, representing intermediate shredded type file
   * It is final because shredded type proved to have JSONPath
   */
  private case class ShreddedDataKeyFinal(key: S3.Key, info: ShreddedType) extends DataKeyFinal {
    def base: S3.Folder = info.info.base
  }

  private type ValidatedDataKeys[F[_]] = F[ValidatedNel[DiscoveryFailure, List[DataKeyFinal]]]
}
