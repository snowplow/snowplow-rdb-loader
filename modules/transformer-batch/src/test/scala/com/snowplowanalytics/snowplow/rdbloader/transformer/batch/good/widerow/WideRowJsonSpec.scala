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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.good.widerow

import com.snowplowanalytics.iglu.core.SchemaCriterion

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage._
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec._

import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Formats.WideRow

import org.specs2.mutable.Specification

abstract class WideRowJsonSpec extends Specification with ShredJobSpec {
  override def appName = "wide-row"
  def skipSchemas: List[SchemaCriterion]
  def inputEventsPath: String
  sequential
  "A job which is configured for wide row json output" should {
    val shreddingComplete = runShredJob(
      events = ResourceFile(inputEventsPath),
      wideRow = Some(WideRow.JSON),
      skipSchemas = skipSchemas
    )

    def schemaCount: Int =
      shreddingComplete.typesInfo match {
        case t: TypesInfo.Shredded => t.types.size
        case t: TypesInfo.WideRow => t.types.size
      }

    def containSkipSchemas: Boolean = {
      val schemas = shreddingComplete.typesInfo match {
        case l: TypesInfo.Shredded => l.types.map(_.schemaKey)
        case l: TypesInfo.WideRow => l.types.map(_.schemaKey)
      }
      schemas.exists(s => ShredJobSpec.inSkipSchemas(skipSchemas, s))
    }

    "transform the enriched event to wide row json" in {
      val Some((lines, _)) = readPartFile(dirs.goodRows)
      val expected = readResourceFile(ResourceFile("/widerow/json/output-widerows"))
      lines.toSet mustEqual (expected.toSet)
    }

    "write bad rows" in {
      val Some((lines, _)) = readPartFile(dirs.badRows)
      val expected = readResourceFile(ResourceFile("/widerow/json/output-badrows"))
        .map(_.replace(VersionPlaceholder, BuildInfo.version))
      lines.toSet mustEqual (expected.toSet)
    }

    "shouldn't contain skipped schemas" in {
      schemaCount mustEqual (21 - skipSchemas.size)
      containSkipSchemas must beFalse
    }
  }
}

class PlainWideRowJsonSpec extends WideRowJsonSpec {
  def skipSchemas = Nil
  def inputEventsPath: String = "/widerow/json/input-events"
}

class SkipSchemasWideRowJsonSpec extends WideRowJsonSpec {
  def skipSchemas = List(
    SchemaCriterion("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", Some(1))
  )
  def inputEventsPath: String = "/widerow/json/input-events-skip-schemas"
}
