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
package config

// This project
import utils.Common._

/**
 * Step is part of loading process or result SQL-statement
 */
sealed trait Step extends Product with Serializable

object Step {

  /**
   * Step that will be skipped if not include it explicitly
   */
  sealed trait IncludeStep extends Step with StringEnum
  case object Vacuum extends IncludeStep { def asString = "vacuum" }

  /** Step that will be included if not skip it explicitly */
  sealed trait SkipStep extends Step with StringEnum
  /** Redshift ANALYZE */
  case object Analyze extends SkipStep { def asString = "analyze" }
  /** Perform additional sleep until S3 is consistent */
  case object ConsistencyCheck extends SkipStep { def asString = "consistency_check" }
  /** Perform defensive check that `etl_tstamp` is not in load manifest yet */
  case object LoadManifestCheck extends SkipStep { def asString = "load_manifest_check" }
  /** Do not COPY data through temporary table, even with `--folder` option (makes sense only with `--folder`) */
  case object TransitCopy extends SkipStep { def asString = "transit_copy" }
  /** Do not perform any interaction with load manifest, including `load_manifest_check` */
  case object LoadManifest extends SkipStep { def asString = "load_manifest" }

  /**
   * Step that cannot be skipped nor included
   */
  sealed trait DefaultStep extends Step
  case object Delete extends DefaultStep
  case object Download extends DefaultStep
  case object Discover extends DefaultStep
  case object Load extends DefaultStep


  implicit val optionalStepRead =
    scopt.Read.reads { (fromString[IncludeStep](_)).andThen { s => s match {
      case Right(x) => x
      case Left(e) => throw new RuntimeException(e)
    } } }

  implicit val skippableStepRead =
    scopt.Read.reads { (fromString[SkipStep](_)).andThen { s => s match {
      case Right(x) => x
      case Left(e) => throw new RuntimeException(e)
    } } }

  /** Steps included into app by default */
  val defaultSteps: Set[Step] = sealedDescendants[SkipStep] ++ Set.empty[Step]

  /**
   * Remove explicitly disabled steps and add optional steps
   * to default set of steps
   *
   * @param toSkip enabled by-default steps
   * @param toInclude disabled by-default steps
   * @return end set of steps
   */
  def constructSteps(toSkip: Set[SkipStep], toInclude: Set[IncludeStep]): Set[Step] = {
    val updated = if (toSkip.contains(Step.LoadManifest))
      defaultSteps - Step.LoadManifestCheck else defaultSteps

    updated -- toSkip ++ toInclude
  }
}
