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
package com.snowplowanalytics.snowplow.rdbloader
package config

import java.util.Base64
import java.nio.charset.StandardCharsets

import cats.Id
import cats.data._
import cats.implicits._

import com.monovore.decline.{Argument, Command, Opts}
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.snowplow.rdbloader.common.{ StorageTarget, StringEnum }

import io.circe.Json
import io.circe.parser.{parse => parseJson}

// This project
import LoaderError._
import generated.ProjectMetadata

/**
 * Validated and parsed result application config
 *
 * @param configYaml decoded Snowplow config.yml
 * @param target decoded target to load
 * @param steps collected steps
 * @param logKey file on S3 to dump logs
 * @param folder specific run-folder to load (skipping discovery)
 * @param dryRun if RDB Loader should just discover data and print SQL
 * @param resolverConfig proven to be valid resolver configuration
  *                       (to not hold side-effecting object)
 */
case class CliConfig(
  configYaml: SnowplowConfig,
  target: StorageTarget,
  steps: Set[Step],
  logKey: Option[S3.Key],
  folder: Option[S3.Folder],
  dryRun: Boolean,
  resolverConfig: Json)

object CliConfig {

  val config = Opts.option[String]("config",
    "base64-encoded string with config.yml content", "c", "config.yml")
  val target = Opts.option[String]("target",
    "base64-encoded string with single storage target configuration JSON", "t", "target.json")
  val resolver = Opts.option[String]("resolver",
    "base64-encoded string with Iglu resolver configuration JSON", "r", "resolver.json")
  val logkey = Opts.option[String]("logkey",
    "S3 key to dump logs", "l", "path").orNone
  val include = Opts.option[Set[Step.IncludeStep]]("include",
    "include optional work steps", "i").withDefault(Set.empty[Step.IncludeStep])
  val skip = Opts.option[Set[Step.SkipStep]]("skip",
    "skip default steps", "s").withDefault(Set.empty[Step.SkipStep])
  val folder = Opts.option[String]("folder",
    "exact run folder to load", metavar = "s3-folder").orNone
  val dryRun = Opts.flag("dry-run", "do not perform loading, just print SQL statements").orFalse

  val rawConfig = (config, target, resolver, logkey, include, skip, folder, dryRun).mapN {
    case (cfg, storage, iglu, log, i, s, fld, dry) => RawConfig(cfg, storage, iglu, i.toSeq, s.toSeq, log, fld, dry)
  }

  val parser = Command[RawConfig](ProjectMetadata.name, ProjectMetadata.version)(rawConfig)

  /**
   * Parse raw CLI arguments into validated and transformed application config
   * This is side-effecting function, it'll print to stdout all errors
   *
   * @param argv list of command-line arguments
   * @return none if not all required arguments were passed
   *         or unknown arguments provided,
   *         some config error if arguments could not be transformed
   *         into application config
   *         some application config if everything was validated
   *         correctly
   */
  def parse(argv: Seq[String]): ValidatedNel[ConfigError, CliConfig] =
    parser.parse(argv).leftMap(help => ConfigError(help.toString)).toValidatedNel.andThen(transform)


  /**
   * Initial raw configuration parsed from CLI arguments
   * Could be invalid, supposed to be validated and transformed
   * into `CliConfig`
   *
   * @param config base64-encoded Snowplow config.yml
   * @param target base64-encoded storage target JSON
   * @param resolver base64-encoded Iglu Resolver JSON
   * @param include sequence of of decoded steps to include
   * @param skip sequence of of decoded steps to skip
   * @param logkey filename, where RDB log dump will be saved
   * @param dryRun if RDB Loader should just discover data and print SQL
   */
  private[config] case class RawConfig(
    config: String,
    target: String,
    resolver: String,
    include: Seq[Step.IncludeStep],
    skip: Seq[Step.SkipStep],
    logkey: Option[String],
    folder: Option[String],
    dryRun: Boolean)

  type Parsed[A] = ValidatedNel[ConfigError, A]

  /** Wrapper for any Base64-encoded entity */
  case class Base64Encoded[A](decode: A)

  implicit def base64EncodedInstance[A: Argument]: Argument[Base64Encoded[A]] = new Argument[Base64Encoded[A]] {
    def read(string: String): ValidatedNel[String, Base64Encoded[A]] = {
      val str = Validated
        .catchOnly[IllegalArgumentException](new String(Base64.getDecoder.decode(string), StandardCharsets.UTF_8))
        .leftMap(_.getMessage)
        .toValidatedNel
      str.andThen(Argument[A].read).map(a => Base64Encoded(a))
    }

    def defaultMetavar: String = "base64"
  }

  implicit def includeStepsArgumentInstance: Argument[Set[Step.IncludeStep]] =
    new Argument[Set[Step.IncludeStep]] {
      def read(string: String): ValidatedNel[String, Set[Step.IncludeStep]] =
        string.split(",").toList.traverse(StringEnum.fromString[Step.IncludeStep](_).toValidatedNel).map(_.toSet)

      def defaultMetavar: String = "steps"
    }

  implicit def skipStepsArgumentInstance: Argument[Set[Step.SkipStep]] =
    new Argument[Set[Step.SkipStep]] {
      def read(string: String): ValidatedNel[String, Set[Step.SkipStep]] =
        string.split(",").toList.traverse(StringEnum.fromString[Step.SkipStep](_).toValidatedNel).map(_.toSet)

      def defaultMetavar: String = "steps"
    }

  /**
   * Validated and transform initial raw cli arguments into
   * ready-to-use `CliConfig`, aggregating errors if any
   *
   * @param rawConfig initial raw arguments
   * @return application config in case of success or
   *         non empty list of config errors in case of failure
   */
  private[config] def transform(rawConfig: RawConfig): ValidatedNel[ConfigError, CliConfig] = {
    val config: Parsed[SnowplowConfig] = base64decode(rawConfig.config).flatMap(SnowplowConfig.parse).toValidatedNel
    val logkey: Parsed[Option[S3.Key]] = rawConfig.logkey.map(k => S3.Key.parse(k).leftMap(ConfigError).toValidatedNel).sequence
    val client: Parsed[(Json, Client[Id, Json])] = loadResolver(rawConfig.resolver).toValidatedNel
    val target: Parsed[StorageTarget] = client.andThen { case (_, r) => loadTarget(r, rawConfig.target) }
    val folder: Parsed[Option[S3.Folder]] = rawConfig.folder.map(f => S3.Folder.parse(f).leftMap(ConfigError).toValidatedNel).sequence
    val steps = Step.constructSteps(rawConfig.skip.toSet, rawConfig.include.toSet)

    (target, config, logkey, folder, client).mapN {
      case (t, c, l, f, (j, _)) => CliConfig(c, t, steps, l, f, rawConfig.dryRun, j)
    }
  }

  /**
   * Safely decode base64 string into plain-text string
   *
   * @param string string, supposed to be base64-encoded
   * @return either error with full description or
   *         plain string in case of success
   */
  private def base64decode(string: String): Either[ConfigError, String] =
    Either
      .catchOnly[IllegalArgumentException](new String(Base64.getDecoder.decode(string), StandardCharsets.UTF_8))
      .leftMap(exception => ConfigError(exception.getMessage))

  /** Decode Iglu Resolver and associated JSON config */
  private def loadResolver(resolverConfigB64: String): Either[ConfigError, (Json, Client[Id, Json])] = {
    base64decode(resolverConfigB64)
      .flatMap(string => parseJson(string).leftMap(error => ConfigError(error.show)))
      .flatMap { json => Client.parseDefault[Id](json).value.leftMap(error => ConfigError(error.show)).map(c => (json, c)) }
  }

  /**
   * Decode and validate base64-encoded storage target config JSON
   *
   * @param resolver working Iglu resolver
   * @param targetConfigB64 base64-encoded storage target JSON
   * @return either aggregated list of errors (from both resolver and target)
   *         or successfully decoded storage target
   */
  private def loadTarget(resolver: Client[Id, Json], targetConfigB64: String): Parsed[StorageTarget] =
    base64decode(targetConfigB64).toValidatedNel.andThen(StorageTarget.parseTarget(resolver, _).toValidated.leftMap {
      errors => errors.map { error => ConfigError(error.message) }
    })
}
