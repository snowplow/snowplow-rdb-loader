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

import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig
import org.specs2.mutable.Specification

class TransformerConfigSpec extends Specification {

  "Formats.overlap" should {
    "confirm two identical criterions overlap" in {
      val criterion = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1))
      TransformerConfig.Formats.Shred.overlap(criterion, criterion) should beTrue
    }

    "confirm two criterions overlap if one of them has * in place where other is concrete" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", None)
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1))
      TransformerConfig.Formats.Shred.overlap(criterionA, criterionB) should beTrue
    }

    "confirm two criterions do not overlap if they have different concrete models" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1))
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(2))
      TransformerConfig.Formats.Shred.overlap(criterionA, criterionB) should beFalse
    }

    "confirm two criterions do not overlap if they have different concrete models, but overlapping revisions" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1), None)
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(2), Some(1))
      TransformerConfig.Formats.Shred.overlap(criterionA, criterionB) should beFalse
    }

    "confirm two criterions do not overlap if they have same concrete models, but different revisions" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1), Some(1))
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1), Some(2))
      TransformerConfig.Formats.Shred.overlap(criterionA, criterionB) should beFalse
    }
  }

  "Formats.findOverlaps" should {
    "find overlapping TSV and JSON" in {
      val criterion = SchemaCriterion("com.acme", "ev", "jsonschema", None)
      TransformerConfig.Formats
        .Shred(LoaderMessage.TypesInfo.Shredded.ShreddedFormat.TSV, List(criterion), List(criterion), List())
        .findOverlaps must beEqualTo(Set(criterion))
    }

    "find overlapping JSON and skip" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", None)
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1))
      TransformerConfig.Formats
        .Shred(LoaderMessage.TypesInfo.Shredded.ShreddedFormat.TSV, List(), List(criterionA), List(criterionB))
        .findOverlaps must beEqualTo(Set(criterionA, criterionB))
    }

    "find overlapping skip and TSV" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", None)
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1))
      val criterionC = SchemaCriterion("com.acme", "unique", "jsonschema", Some(1))
      TransformerConfig.Formats
        .Shred(LoaderMessage.TypesInfo.Shredded.ShreddedFormat.TSV, List(criterionA), List(criterionC), List(criterionB))
        .findOverlaps must beEqualTo(Set(criterionA, criterionB))
    }

    "not find anything if not overlaps" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1))
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(2))
      val criterionC = SchemaCriterion("com.acme", "ev", "jsonschema", Some(3))
      TransformerConfig.Formats
        .Shred(LoaderMessage.TypesInfo.Shredded.ShreddedFormat.TSV, List(criterionA), List(criterionB), List(criterionC))
        .findOverlaps must beEmpty
    }
  }
}
