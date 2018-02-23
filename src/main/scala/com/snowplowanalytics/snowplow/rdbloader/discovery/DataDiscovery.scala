/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
package discovery

import cats.data._
import cats.free.Free
import cats.implicits._

import com.snowplowanalytics.manifest.core.Item

import com.snowplowanalytics.snowplow.rdbloader.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.LoaderError._

/**
  * Result of data discovery in shredded.good folder
  * @param base ahred run folder full path
  * @param atomicCardinality amount of keys in atomic-events directory
  * @param shreddedTypes list of shredded types in this directory
  * @param specificFolder if specific target loader was provided by `--folder`
  *                       (remains default `false` until `setSpecificFolder`)
  * @param item Processing Manifest records if it was discovered through manifest
  */
case class DataDiscovery(
    base: S3.Folder,
    atomicCardinality: Option[Long],
    shreddedTypes: List[ShreddedType],
    specificFolder: Boolean,
    item: Option[Item]) {
  /** ETL id */
  def runId: String = base.split("/").last

  /** `atomic-events` directory full path */
  def atomicEvents: S3.Folder =
    S3.Folder.append(base, "atomic-events")

  /**
   * Time in ms for run folder to setup eventual consistency,
   * based on amount of atomic and shredded files
   */
  def consistencyTimeout: Long =
    ((atomicCardinality * 0.1 * shreddedTypes.length).toLong + 5L) * 1000

  def show: String =
    s"$runId with $atomicCardinality atomic files and following shredded types:\n${shreddedTypes.map(t => "  + " + t.show).mkString("\n")}"
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

  /** Amount of times consistency check will be performed */
  val ConsistencyChecks = 5

  /**
   * ADT indicating whether shredded.good or arbitrary folder
   * `InSpecificFolder` results on discovery error on empty folder
   * `InShreddedGood` results in noop on empty folder
   */
  sealed trait DiscoveryTarget extends Product with Serializable
  case class Global(folder: S3.Folder) extends DiscoveryTarget
  case class InSpecificFolder(folder: S3.Folder) extends DiscoveryTarget
  case class ViaManifest(folder: Option[S3.Folder]) extends DiscoveryTarget

  /**
   * Discover list of shred run folders, each containing
   * exactly one `atomic-events` folder and zero or more `ShreddedType`s
   *
   * Action fails if:
   * + no atomic-events folder found in *any* shred run folder
   * + files with unknown path were found in *any* shred run folder
   *
   * @param target either shredded good or specific run folder
   * @param id storage target id to avoid "re-discovering" target when using manifest
   * @param shredJob shred job version to check path pattern
   * @param region AWS region for S3 buckets
   * @param assets optional JSONPath assets S3 bucket
   * @return list (probably empty, but usually with single element) of discover results
   *         (atomic events and shredded types)
   */
  def discoverFull(target: DiscoveryTarget,
                   id: String,
                   shredJob: Semver,
                   region: String,
                   assets: Option[S3.Folder]): LoaderAction[List[DataDiscovery]] = {
    def group(validatedDataKeys: LoaderAction[ValidatedDataKeys]): LoaderAction[List[DataDiscovery]] =
      for {
        keys <- validatedDataKeys
        discovery <- groupKeysFull(keys)
      } yield discovery

    target match {
      case Global(folder) =>
        val keys: LoaderAction[ValidatedDataKeys] =
          listGoodBucket(folder).map(transformKeys(shredJob, region, assets))
        group(keys)
      case InSpecificFolder(folder) =>
        val keys: LoaderAction[ValidatedDataKeys] =
          listGoodBucket(folder).map { keys =>
            if (keys.isEmpty) {
              val failure = Validated.Invalid(NonEmptyList(NoDataFailure(folder), Nil))
              Free.pure(failure)
            } else transformKeys(shredJob, region, assets)(keys)
          }
        group(keys)
      case ViaManifest(None) =>
        ManifestDiscovery.discover(id, region, assets)
      case ViaManifest(Some(folder)) =>
        ManifestDiscovery.discoverFolder(folder, id, region, assets).map(_.pure[List])
    }
  }

  /**
   * List whole directory excluding special files
   */
  def listGoodBucket(folder: S3.Folder): LoaderAction[List[S3.Key]] =
    EitherT(LoaderA.listS3(folder)).map(_.filterNot(isSpecial))

  // Full discovery

  /**
   * Group list of keys into list (usually single-element) of `DataDiscovery`
   *
   * @param validatedDataKeys IO-action producing validated list of `FinalDataKey`
   * @return IO-action producing list of
   */
  def groupKeysFull(validatedDataKeys: ValidatedDataKeys): LoaderAction[List[DataDiscovery]] = {
    def group(dataKeys: List[DataKeyFinal]): ValidatedNel[DiscoveryFailure, List[DataDiscovery]] =
      dataKeys.groupBy(_.base).toList.reverse.traverse(validateFolderFull)

    // Transform into Either with non-empty list of errors
    val result: Action[Either[LoaderError, List[DataDiscovery]]] =
      validatedDataKeys.map { keys =>
        keys.andThen(group) match {
          case Validated.Valid(discovery) =>
            discovery.asRight
          case Validated.Invalid(failures) =>
            val aggregated = LoaderError.aggregateDiscoveryFailures(failures.toList).distinct
            DiscoveryError(aggregated).asLeft
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
    val (atomicKeys, shreddedKeys) = dataKeys.foldLeft(empty) { case ((atomicKeys, shreddedTypes), key) =>
      key match {
        case atomicKey: AtomicDataKey => (atomicKey :: atomicKeys, shreddedTypes)
        case shreddedType: ShreddedDataKeyFinal => (atomicKeys, shreddedType :: shreddedTypes)
      }
    }

    if (atomicKeys.nonEmpty) {
      val shreddedData = shreddedKeys.map(_.info).distinct
      DataDiscovery(base, Some(atomicKeys.length), shreddedData, false, None).validNel
    } else {
      AtomicDiscoveryFailure(base).invalidNel
    }
  }

  /**
   * Transform list of S3 keys into list of `DataKeyFinal` for `DataDiscovery`
   */
  private def transformKeys(shredJob: Semver, region: String, assets: Option[S3.Folder])(keys: List[S3.Key]): ValidatedDataKeys = {
    def id(x: ValidatedNel[DiscoveryFailure, List[DataKeyFinal]]) = x

    val intermediateDataKeys = keys.map(parseDataKey(shredJob, _))
    val finalDataKeys = intermediateDataKeys.traverse(transformDataKey(_, region, assets))
    sequenceInF(finalDataKeys, id)
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
   * @return `Action` conaining `Validation` - as on next step we can aggregate errors
   */
  private def transformDataKey(
      dataKey: DiscoveryStep[DataKeyIntermediate],
      region: String,
      assets: Option[S3.Folder]
  ): Action[ValidatedNel[DiscoveryFailure, DataKeyFinal]] = {
    dataKey match {
      case Right(ShreddedDataKeyIntermediate(fullPath, info)) =>
        val jsonpathAction = EitherT(ShreddedType.discoverJsonPath(region, assets, info))
        val discoveryAction = jsonpathAction.map { jsonpath =>
          ShreddedDataKeyFinal(fullPath, ShreddedType(info, jsonpath))
        }
        discoveryAction.value.map(_.toValidatedNel)
      case Right(AtomicDataKey(fullPath)) =>
        val pure: Action[ValidatedNel[DiscoveryFailure, DataKeyFinal]] =
          Free.pure(AtomicDataKey(fullPath).validNel[DiscoveryFailure])
        pure
      case Left(failure) =>
        val pure: Action[ValidatedNel[DiscoveryFailure, DataKeyFinal]] =
          Free.pure(failure.invalidNel)
        pure
    }
  }

  /**
   * Parse S3 key into valid atomic-events path or shredded type
   * This function will short-circuit whole discover process if found
   * any invalid S3 key - not atomic events file neither shredded type file
   *
   * @param shredJob shred job version to check path pattern
   * @param key particular S3 key in shredded good folder
   */
  private def parseDataKey(shredJob: Semver, key: S3.Key): Either[DiscoveryFailure, DataKeyIntermediate] = {
    S3.getAtomicPath(key) match {
      case Some(_) => AtomicDataKey(key).asRight
      case None =>
        ShreddedType.transformPath(key, shredJob) match {
          case Right(info) =>
            ShreddedDataKeyIntermediate(key, info).asRight
          case Left(e) => e.asLeft
        }
    }
  }

  // Common

  def isAtomic(key: S3.Key): Boolean = key match {
    case loaders.Common.atomicSubpathPattern(_, _, _) => true
    case _ => false
  }

  def isSpecial(key: S3.Key): Boolean = key.contains("$") || key == "_SUCCESS"

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
  def checkConsistency(originalAction: LoaderAction[List[DataDiscovery]]): LoaderAction[List[DataDiscovery]] = {
    def check(checkAttempt: Int, last: Option[Either[LoaderError, List[DataDiscovery]]]): ActionE[List[DataDiscovery]] = {
      val action = last.map(Free.pure[LoaderA, Either[LoaderError, List[DataDiscovery]]]).getOrElse(originalAction.value)

      for {
        original <- action
        _        <- sleepConsistency(original)
        control  <- originalAction.value
        result   <- retry(original, control, checkAttempt + 1)
      } yield result
    }

    def retry(
      original: Either[LoaderError, List[DataDiscovery]],
      control: Either[LoaderError, List[DataDiscovery]],
      attempt: Int): ActionE[List[DataDiscovery]] = {
      (original, control) match {
        case _ if attempt >= ConsistencyChecks =>
          for {
            _ <- LoaderA.print(s"Consistency check did not pass after $ConsistencyChecks attempts")
            discovered <- Free.pure(control.orElse(original))
          } yield discovered
        case (Right(o), Right(c)) if o.sortBy(_.base.toString) == c.sortBy(_.base.toString) =>
          val message = o.map(x => s"+ ${x.show}").mkString("\n")
          for {
            _ <- LoaderA.print(s"Consistency check passed after ${attempt - 1} attempt. Following run ids found:\n$message")
            discovered <- Free.pure(original)
          } yield discovered
        case (Left(_), Left(_)) =>
          for {
            _ <- LoaderA.print(s"Consistency check failed. Making another attempt")
            next <- check(attempt, None)
          } yield next
        case (Right(o), Right(c)) =>
          val message = if (attempt == ConsistencyChecks - 1)
            s"Difference:\n ${discoveryDiff(o, c).map(m => s"+ $m").mkString("\n")}"
          else ""

          for {
            _ <- LoaderA.print(s"Consistency check failed. $message")
            next <- check(attempt, Some(control))
          } yield next
      }
    }

    EitherT[Action, LoaderError, List[DataDiscovery]](check(1, None))
  }

  def discoveryDiff(original: List[DataDiscovery], control: List[DataDiscovery]): List[String] = {
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
  private def sleepConsistency(result: Either[LoaderError, List[DataDiscovery]]): Action[Unit] = {
    val timeoutMs = result match {
      case Right(list) =>
        list.map(_.consistencyTimeout).foldLeft(10000L)(_ + _)
      case Left(_) => 10000L
    }

    LoaderA.sleep(timeoutMs)
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
  private case class AtomicDataKey(key: S3.Key) extends DataKeyIntermediate with DataKeyFinal {
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

  /**
   * S3 key, representing intermediate shredded type file
   * It is final because shredded type proved to have JSONPath
   */
  private case class ShreddedDataKeyFinal(key: S3.Key, info: ShreddedType) extends DataKeyFinal {
    def base: S3.Folder = info.info.base
  }

  private type ValidatedDataKeys = Action[ValidatedNel[DiscoveryFailure, List[DataKeyFinal]]]
}
