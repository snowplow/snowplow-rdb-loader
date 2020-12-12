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

import java.util.UUID

import scala.io.Source.fromInputStream

import cats.Id

import io.circe.jawn.parse

import com.snowplowanalytics.iglu.core.SchemaCriterion

import com.snowplowanalytics.iglu.client.Resolver

import com.snowplowanalytics.snowplow.rdbloader.common.{S3, StorageTarget, Config, LoaderMessage}
import com.snowplowanalytics.snowplow.rdbloader.loaders.Common.SqlString

object SpecHelpers {

  val resolverStream = getClass.getResourceAsStream("/resolver.json.base64")
  val resolverConfig = fromInputStream(resolverStream).getLines.mkString("\n")
  val resolverJson = parse(new String(java.util.Base64.getDecoder.decode(resolverConfig))).getOrElse(throw new RuntimeException("Invalid resolver.json"))
  val resolver = Resolver.parse[Id](resolverJson).toOption.getOrElse(throw new RuntimeException("Invalid resolver config"))

  val disableSsl = StorageTarget.RedshiftJdbc.empty.copy(ssl = Some(false))
  val validTarget = StorageTarget.Redshift(
    "angkor-wat-final.ccxvdpz01xnr.us-east-1.redshift.amazonaws.com",
    "snowplow",
    5439,
    disableSsl,
    "arn:aws:iam::123456789876:role/RedshiftLoadRole",
    "atomic",
    "admin",
    StorageTarget.PasswordConfig.PlainText("Supersecret1"),
    1,
    20000,
    None)

  val validConfig: Config[StorageTarget] = Config(
    "Acme Redshift",
    UUID.fromString("123e4567-e89b-12d3-a456-426655440000"),
    "us-east-1",
    None,
    Config.OutputCompression.Gzip,
    Config.Monitoring(
      Some(Config.SnowplowMonitoring("redshift-loader","snplow.acme.com")),
      None
    ),
    "messages",
    StorageTarget.Redshift(
      "redshift.amazon.com",
      "snowplow",
      5439,
      StorageTarget.RedshiftJdbc(None, None, None, None, None, None, None, Some(true),None,None,None,None),
      "${role_arn}",
      "atomic",
      "storage-loader",
      StorageTarget.PasswordConfig.PlainText("secret"),
      10,
      100000,
      None
    ),
    Config.Formats(
      LoaderMessage.Format.TSV,
      List(
        SchemaCriterion("com.acme","tsv-event","jsonschema",Some(1),None,None),
        SchemaCriterion("com.acme","tsv-event","jsonschema",Some(2),None,None)
      ),
      List(SchemaCriterion("com.acme","json-event","jsonschema",Some(1),Some(0),Some(0))),
      List(SchemaCriterion("com.acme","skip-event","jsonschema",Some(1),None,None))
    ),
    Set.empty
  )

  val validTargetWithManifest = StorageTarget.Redshift(
    "angkor-wat-final.ccxvdpz01xnr.us-east-1.redshift.amazonaws.com",
    "snowplow",
    5439,
    disableSsl,
    "arn:aws:iam::123456789876:role/RedshiftLoadRole",
    "atomic",
    "admin",
    StorageTarget.PasswordConfig.PlainText("Supersecret1"),
    1,
    20000,
    None
  )

  /**
    * Pretty prints a Scala value similar to its source represention.
    * Particularly useful for case classes.
    * @param a - The value to pretty print.
    * @param indentSize - Number of spaces for each indent.
    * @param maxElementWidth - Largest element size before wrapping.
    * @param depth - Initial depth to pretty print indents.
    * @author https://gist.github.com/carymrobbins/7b8ed52cd6ea186dbdf8
    */
  def prettyPrint(a: Any, indentSize: Int = 2, maxElementWidth: Int = 30, depth: Int = 0): String = {
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
    def sql: SqlString = SqlString.unsafeCoerce(s)
    def dir: S3.Folder = S3.Folder.coerce(s)
  }
}
