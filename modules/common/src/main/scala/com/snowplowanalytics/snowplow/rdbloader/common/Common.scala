/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.common

import com.snowplowanalytics.iglu.core.{SchemaVer, SchemaKey}

import com.snowplowanalytics.iglu.client.resolver.registries.Registry

import com.snowplowanalytics.iglu.schemaddl.redshift._

import com.snowplowanalytics.snowplow.rdbloader.common.config.Config.Formats
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{ShreddedType, Format}

/**
 * Various common utility functions
 */
object Common {

  val GoodPrefix = "output=good"

  val AtomicSchema: SchemaKey =
    SchemaKey("com.snowplowanalytics.snowplow", "atomic", "jsonschema", SchemaVer.Full(1,0,0))
  val AtomicType = ShreddedType(AtomicSchema, Format.TSV)
  val AtomicPath: String = entityPath(AtomicType)

  def entityPath(entity: ShreddedType) =
    s"$GoodPrefix/vendor=${entity.schemaKey.vendor}/name=${entity.schemaKey.name}/format=${entity.format.path}/model=${entity.schemaKey.version.model}"

  def entityPathFull(base: S3.Folder, entity: ShreddedType): S3.Folder =
    S3.Folder.append(base, entityPath(entity))

  /**
   * Remove all occurrences of access key id and secret access key from message
   * Helps to avoid publishing credentials on insecure channels
   *
   * @param message original message that may contain credentials
   * @param stopWords list of secret words (such as passwords) that should be sanitized
   * @return string with hidden keys
   */
  def sanitize(message: String, stopWords: List[String]): String =
    stopWords.foldLeft(message) { (result, secret) =>
      result.replace(secret, "x" * secret.length)
    }

  /**
   * Transforms CamelCase string into snake_case
   * Also replaces all hyphens with underscores
   *
   * @see https://github.com/snowplow/iglu/blob/master/0-common/schema-ddl/src/main/scala/com.snowplowanalytics/iglu.schemaddl/StringUtils.scala
   * @param str string to transform
   * @return the underscored string
   */
  def toSnakeCase(str: String): String =
    str.replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
      .replaceAll("([a-z\\d])([A-Z])", "$1_$2")
      .replaceAll("-", "_")
      .replaceAll("""\.""", "_")
      .toLowerCase

  def isTabular(formats: Formats)(schemaKey: SchemaKey): Boolean =
    formats.default match {
      case LoaderMessage.Format.TSV =>
        val notJson = !formats.json.exists(c => c.matches(schemaKey))
        val notSkip = !formats.skip.exists(c => c.matches(schemaKey))
        notJson && notSkip
      case LoaderMessage.Format.JSON =>
        formats.tsv.exists(c => c.matches(schemaKey))
    }

  /** Registry embedded into RDB Loader jar */
  private val loaderRefConf = Registry.Config("RDB Loader Embedded", 0, List("com.snowplowanalytics.snowplow.rdbloader"))
  val LoaderRegistry = Registry.Embedded(loaderRefConf, "/com.snowplowanalytics.snowplow.rdbloader/embedded-registry")

  /**
   * Extract integer from string if it contains only valid number
   */
  object IntString {
    def unapply(s: String): Option[Int] =
      try { Some(s.toInt) } catch { case _: NumberFormatException => None }
  }

  /**
   * Create AddColumn statement from given column
   * @param column Column to add
   * @param default optional default value for column
   * @return AddColumn statement created from given column
   */
  def toAddColumn(column: Column, default: Option[String]): AddColumn =
    column match {
      case Column(columnName, dataType, columnAttributes, columnConstraints) =>
        val encoding: Option[CompressionEncoding] = columnAttributes.collectFirst { case c: CompressionEncoding => c }
        val nullability: Option[Nullability] = columnConstraints.collectFirst { case n: Nullability => n}
        AddColumn(columnName, dataType, default.map(Default.apply), encoding, nullability)
    }
}
