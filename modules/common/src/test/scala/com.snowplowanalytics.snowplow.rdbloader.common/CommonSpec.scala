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
package com.snowplowanalytics.snowplow.rdbloader.common

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer}

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig

import org.specs2.mutable.Specification

class CommonSpec extends Specification {
  "isTabular" should {
    "respect default TSV even if SchemaKey is not listed" in {
      val input = SchemaKey("com.acme", "tsv-not-listed", "jsonschema", SchemaVer.Full(1, 0, 0))
      val result = Common.isTabular(CommonSpec.formats)(input)
      result should beTrue
    }

    "respect default JSON" in {
      val input = SchemaKey("com.acme", "tsv-not-listed", "jsonschema", SchemaVer.Full(1, 0, 0))
      val jsonFormat = CommonSpec.formats.copy(default = TypesInfo.Shredded.ShreddedFormat.JSON)
      val result = Common.isTabular(jsonFormat)(input)
      result should beFalse
    }

    "respect keys listed in json" in {
      val input = SchemaKey("com.acme", "json-event", "jsonschema", SchemaVer.Full(1, 0, 0))
      val result = Common.isTabular(CommonSpec.formats)(input)
      result should beFalse
    }

    "respect keys listed in skip" in {
      val input = SchemaKey("com.acme", "skip-event", "jsonschema", SchemaVer.Full(1, 0, 0))
      val result = Common.isTabular(CommonSpec.formats)(input)
      result should beFalse
    }
  }
}

object CommonSpec {

  val formats = TransformerConfig.Formats.Shred(
    TypesInfo.Shredded.ShreddedFormat.TSV,
    List(
      SchemaCriterion("com.acme", "tsv-event", "jsonschema", Some(1), None, None),
      SchemaCriterion("com.acme", "tsv-event", "jsonschema", Some(2), None, None)
    ),
    List(SchemaCriterion("com.acme", "json-event", "jsonschema", Some(1), Some(0), Some(0))),
    List(SchemaCriterion("com.acme", "skip-event", "jsonschema", Some(1), None, None))
  )
}
