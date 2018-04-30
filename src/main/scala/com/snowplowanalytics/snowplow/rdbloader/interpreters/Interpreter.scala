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
package interpreters

import cats._

import implementations.{S3Interpreter, TrackerInterpreter}

// This project
import config.CliConfig

trait Interpreter {
  def run: LoaderA ~> Id
}

object Interpreter {

  /**
    * Initialize clients/connections for interpreter and interpreter itself
    *
    * @param cliConfig RDB Loader app configuration
    * @return prepared interpreter
    */
  def initialize(cliConfig: CliConfig): Interpreter = {
    val resolver = utils.Compat.convertIgluResolver(cliConfig.resolverConfig)
      .fold(x => throw new RuntimeException(s"Initialization error. Cannot initialize Iglu Resolver. ${x.toList.mkString(", ")}"), r => r)
    val amazonS3 = S3Interpreter.getClient(cliConfig.configYaml.aws)
    val tracker = TrackerInterpreter.initializeTracking(cliConfig.configYaml.monitoring)

    if (cliConfig.dryRun) {
      new DryRunInterpreter(cliConfig, amazonS3, tracker, resolver)
    } else {
      new RealWorldInterpreter(cliConfig, amazonS3, tracker, resolver)
    }
  }
}
