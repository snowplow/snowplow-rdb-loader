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

import cats.{Applicative, Id, Monad}
import cats.effect.Clock
import com.snowplowanalytics.iglu.client.resolver.registries.{Registry, RegistryError, RegistryLookup}
import com.snowplowanalytics.iglu.client.resolver.{Resolver, StorageTime}
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaList, SchemaVer}
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModel
import com.snowplowanalytics.lrumap.CreateLruMap
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{ShredModelCache, Transformed}
import io.circe.Json
import io.circe.literal.JsonStringContext
import org.specs2.mutable.Specification

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import java.util.concurrent.TimeUnit

class CachedShredModelSpec extends Specification {

  val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "test_schema", "jsonschema", SchemaVer.Full(1, 0, 0))

  val data = json"""{ "field1": "1" }"""

  val schema100: Json =
    json"""
         {
             "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
             "description": "Test schema 1",
             "self": {
                 "vendor": "com.snowplowanalytics.snowplow",
                 "name": "test_schema",
                 "format": "jsonschema",
                 "version": "1-0-0"
             },

             "type": "object",
             "properties": {
               "field1": { "type": "string"}
              }
         }
        """

  "Cached shred model" should {
    "original schema only, 1 field flattened" in {

      val cache = getCache
      val result: ShredModel = helper(schemaKey, cache, 1000, schema100)

      result.jsonToStrings(data) must beEqualTo(List("1"))
      cache.get((schemaKey, FiniteDuration(1000, TimeUnit.MILLISECONDS))) must beSome
    }
  }
  private def helper(
    schemaKey: SchemaKey,
    shredModelCache: ShredModelCache[Id],
    currentTime: Long,
    schemaInRegistry: Json
  ): ShredModel = {

    // To return value stored in the schemaInRegistry variable, passed registry is ignored
    val testRegistryLookup: RegistryLookup[Id] = new RegistryLookup[Id] {
      override def lookup(registry: Registry, schemaKey: SchemaKey): Id[Either[RegistryError, Json]] =
        Right(schemaInRegistry)

      override def list(
        registry: Registry,
        vendor: String,
        name: String,
        model: Int
      ): Id[Either[RegistryError, SchemaList]] =
        Right(SchemaList(List(SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/test_schema/jsonschema/1-0-0").right.get)))
    }

    val clock: Clock[Id] = new Clock[Id] {
      override def applicative: Applicative[Id] = Applicative[Id]

      override def monotonic: Id[FiniteDuration] = FiniteDuration(currentTime * 1000000, TimeUnit.NANOSECONDS)

      override def realTime: Id[FiniteDuration] = FiniteDuration(currentTime, TimeUnit.MILLISECONDS)
    }

    Transformed.lookupShredModel[Id](schemaKey, shredModelCache, getResolver)(Monad[Id], clock, testRegistryLookup).value.right.get
  }

  private def getCache: ShredModelCache[Id] = CreateLruMap[Id, (SchemaKey, StorageTime), ShredModel].create(100)

  private def getResolver: Resolver[Id] =
    Resolver.init[Id](
      cacheSize = 10,
      cacheTtl = Some(10.seconds),
      refs = Registry.Embedded( // not used in test as we fix returned schema in custom test RegistryLookup
        Registry.Config("Test", 0, List.empty),
        path = "/fake"
      )
    )
}
