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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import cats.Id

import io.circe.literal._

import org.apache.spark.util.{LongAccumulator, SizeEstimator}

import org.specs2.mutable.Specification
import org.specs2.specification.core.Fragment

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.JavaNetRegistryLookup._

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.rdbloader.common.catsClockIdInstance
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.{AtomicFieldsProvider, NonAtomicFieldsProvider}
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields.AllFields
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.good.{EmptySchemaSpec, LinkClickEventSpec, MultipleJsonsSpec}
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark.TypesAccumulator
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark.singleton.IgluSingleton

/**
 * The transform runs on events still held by the natural-deduplication aggregation, which Spark has
 * already sized to stay within its memory budget. If the transform grows those events, the heap
 * fills while Spark believes it is within budget.
 *
 * A little growth is harmless: the JDK caches a small view object inside each JSON object that is
 * iterated, so wide row output grows an event by tens of bytes. What matters is that Spark's
 * measurement stays accurate to within a couple of percent.
 */
class HeldEventSizeSpec extends Specification {

  private val ToleratedGrowth = 0.02

  private val resolverConfig = Resolver
    .parseConfig(json"""{
      "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
      "data": {
        "cacheSize": 500,
        "repositories": [
          {
            "name": "Iglu Central",
            "priority": 0,
            "vendorPrefixes": [ "com.snowplowanalytics" ],
            "connection": { "http": { "uri": "https://com-iglucentral-eu1-prod.iglu.snplow.net/api" } }
          }
        ]
      }
    }""")
    .fold(e => throw new IllegalArgumentException(e.getMessage), identity)

  // An unstruct event; contexts plus an unstruct event; and contexts whose entities have no data,
  // where the per-JSON-object growth is the largest share of the event
  private val fixtures = List(
    "a link click event" -> LinkClickEventSpec.lines.lines.head,
    "an event with contexts and an unstruct event" -> MultipleJsonsSpec.lines.lines.head,
    "an event with empty contexts" -> EmptySchemaSpec.lines.lines.head
  )

  private def parse(line: String): Event =
    Event.parse(line).toEither.fold(e => throw new IllegalArgumentException(e.toString), identity)

  private def sizeKeptBy(line: String, transformer: Transformer[_]) = {
    val event  = parse(line)
    val before = SizeEstimator.estimate(event)
    transformer.goodTransform(event, new LongAccumulator, new LongAccumulator) must not(beEmpty)
    val after = SizeEstimator.estimate(event)
    after must beBetween(before, before + (before * ToleratedGrowth).toLong)
  }

  private def shredTransformer =
    Transformer.ShredTransformer(
      resolverConfig,
      TransformerConfig.Formats.Shred(TypesInfo.Shredded.ShreddedFormat.TSV, Nil, Nil, Nil),
      0,
      Nil,
      exitOnMissingIgluSchema = true
    )

  private def parquetTransformer(line: String) = {
    val types           = parse(line).inventory.map(TypesAccumulator.wideRowTypeConverter).toList
    val nonAtomicFields = NonAtomicFieldsProvider.build[Id](IgluSingleton.get(resolverConfig), types).value.toOption.get
    val allFields       = AllFields(AtomicFieldsProvider.static, nonAtomicFields)
    Transformer.WideRowParquetTransformer(allFields, SparkSchema.build(allFields))
  }

  private val outputs: List[(String, String => Transformer[_])] = List(
    "shredded output" -> (_ => shredTransformer),
    "wide row JSON output" -> (_ => Transformer.WideRowJsonTransformer()),
    "wide row Parquet output" -> parquetTransformer
  )

  "goodTransform" should {
    Fragment.foreach(for {
      (event, line) <- fixtures
      (output, transformer) <- outputs
    } yield (event, output, line, transformer)) { case (event, output, line, transformer) =>
      s"keep Spark's measurement of $event accurate, for $output" in sizeKeptBy(line, transformer(line))
    }
  }
}
