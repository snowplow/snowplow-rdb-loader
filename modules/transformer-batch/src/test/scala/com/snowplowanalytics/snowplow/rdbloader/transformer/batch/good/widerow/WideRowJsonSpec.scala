/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
