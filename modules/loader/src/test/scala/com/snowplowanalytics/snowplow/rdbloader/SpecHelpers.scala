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
package com.snowplowanalytics.snowplow.rdbloader

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import doobie.util.fragment.Fragment
import doobie.util.update.Update0
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.Base64

object SpecHelpers {

  val validConfig: Config[StorageTarget.Redshift] = Config(
    ConfigSpec.exampleRedshift,
    ConfigSpec.exampleCloud,
    ConfigSpec.exampleJsonPaths,
    ConfigSpec.exampleMonitoring,
    ConfigSpec.exampleRetryQueue,
    ConfigSpec.exampleSchedules,
    ConfigSpec.exampleTimeouts,
    ConfigSpec.exampleRetries,
    ConfigSpec.exampleReadyCheck,
    ConfigSpec.exampleInitRetries,
    ConfigSpec.exampleFeatureFlags,
    ConfigSpec.exampleTelemetry
  )

  def asB64(resourcePath: String): String =
    encode(readResource(resourcePath))

  def readResource(resourcePath: String): String =
    Files.readString(fullPathOf(resourcePath))

  def encode(string: String): String =
    new String(Base64.getEncoder.encode(string.getBytes(StandardCharsets.UTF_8)))

  def fullPathOf(resource: String): Path =
    Paths.get(getClass.getResource(resource).toURI)

  /**
   * Pretty prints a Scala value similar to its source represention. Particularly useful for case
   * classes.
   * @param a
   *   \- The value to pretty print.
   * @param indentSize
   *   \- Number of spaces for each indent.
   * @param maxElementWidth
   *   \- Largest element size before wrapping.
   * @param depth
   *   \- Initial depth to pretty print indents.
   * @author
   *   https://gist.github.com/carymrobbins/7b8ed52cd6ea186dbdf8
   */
  def prettyPrint(
    a: Any,
    indentSize: Int = 2,
    maxElementWidth: Int = 30,
    depth: Int = 0
  ): String = {
    val indent = " " * depth * indentSize
    val fieldIndent = indent + (" " * indentSize)
    val thisDepth = prettyPrint(_: Any, indentSize, maxElementWidth, depth)
    val nextDepth = prettyPrint(_: Any, indentSize, maxElementWidth, depth + 1)
    a match {
      // Make Strings look similar to their literal form.
      case s: String =>
        val replaceMap = Seq(
          "\n" -> "\\n",
          "\r" -> "\\r",
          "\t" -> "\\t",
          "\"" -> "\\\""
        )
        '"' + replaceMap.foldLeft(s) { case (acc, (c, r)) => acc.replace(c, r) } + '"'
      // For an empty Seq just use its normal String representation.
      case xs: Seq[_] if xs.isEmpty => xs.toString()
      case xs: Seq[_] =>
        // If the Seq is not too long, pretty print on one line.
        val resultOneLine = xs.map(nextDepth).toString()
        if (resultOneLine.length <= maxElementWidth) return resultOneLine
        // Otherwise, build it with newlines and proper field indents.
        val result = xs.map(x => s"\n$fieldIndent${nextDepth(x)}").toString()
        result.substring(0, result.length - 1) + "\n" + indent + ")"
      // Product should cover case classes.
      case p: Product =>
        val prefix = p.productPrefix
        // We'll use reflection to get the constructor arg names and values.
        val cls = p.getClass
        val fields = cls.getDeclaredFields.filterNot(_.isSynthetic).map(_.getName)
        val values = p.productIterator.toSeq
        // If we weren't able to match up fields/values, fall back to toString.
        if (fields.length != values.length) return p.toString
        fields.zip(values).toList match {
          // If there are no fields, just use the normal String representation.
          case Nil => p.toString
          // If there is just one field, let's just print it as a wrapper.
          case (_, value) :: Nil => s"$prefix(${thisDepth(value)})"
          // If there is more than one field, build up the field names and values.
          case kvps =>
            val prettyFields = kvps.map { case (k, v) => s"$fieldIndent$k = ${nextDepth(v)}" }
            // If the result is not too long, pretty print on one line.
            val resultOneLine = s"$prefix(${prettyFields.mkString(", ")})"
            if (resultOneLine.length <= maxElementWidth) return resultOneLine
            // Otherwise, build it with newlines and proper field indents.
            s"$prefix(\n${prettyFields.mkString(",\n")}\n$indent)"
        }
      // If we haven't specialized this type, just use its toString.
      case _ => a.toString
    }
  }

  implicit class AsSql(s: String) {
    def sql: Update0 = Fragment.const0(s).update
    def dir: BlobStorage.Folder = BlobStorage.Folder.coerce(s)
    def key: BlobStorage.Key = BlobStorage.Key.coerce(s)
  }
}
