package com.snowplowanalytics.snowplow.rdbloader.common

import cats.effect.Clock
import cats.{Id, Monad}
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.{Registry, RegistryError, RegistryLookup}
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaList, SelfDescribingData}
import com.snowplowanalytics.iglu.schemaddl.Properties
import com.snowplowanalytics.lrumap.CreateLruMap
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{EventUtils, PropertiesCache, PropertiesKey}
import io.circe.Json
import io.circe.literal.JsonStringContext
import org.specs2.mutable.Specification

import scala.concurrent.duration.{MILLISECONDS, NANOSECONDS, TimeUnit}

class CachedFlatteningSpec extends Specification {

  // single 'field1' field
  val `original schema - 1 field`: Json =
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

  // same key as schema1, but with additional `field2` field.
  val `patched schema - 2 fields`: Json =
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
               "field1": { "type": "string" },
               "field2": { "type": "integer" }
              } 
         }
        """

  val cacheTtl = 10 // seconds
  val dataToFlatten = json"""{ "field1": "1", "field2": 2 }"""
  val schemaKey = "iglu:com.snowplowanalytics.snowplow/test_schema/jsonschema/1-0-0"

  // (vendor, name, model)
  val propertiesKey = ("com.snowplowanalytics.snowplow", "test_schema", 1)

  "Cached properties during flattening should be in sync with cached schemas/lists in iglu client" >> {

    "(1) original schema only, 1 flatten call => 1 field flattened" in {
      val propertiesCache = getCache
      val result = flatten(propertiesCache, getResolver)(
        currentTime = 1000, // ms
        schemaInRegistry = `original schema - 1 field`
      )

      result must beEqualTo(List("1"))

      // Properties are cached after first call (1 second)
      propertiesCache.get((propertiesKey, 1)) must beSome
    }

    "(2) original schema is patched between calls, no delay => original schema is still cached => 1 field flattened" in {
      val propertiesCache = getCache
      val resolver = getResolver

      // first call
      flatten(propertiesCache, resolver)(
        currentTime = 1000, // ms
        schemaInRegistry = `original schema - 1 field`
      )

      // second call, same time
      val result = flatten(propertiesCache, resolver)(
        currentTime = 1000, // ms
        schemaInRegistry = `patched schema - 2 fields` // different schema with the same key!
      )

      // no data from patched schema
      result must beEqualTo(List("1"))

      // Properties are cached after first call (1 second)
      propertiesCache.get((propertiesKey, 1)) must beSome
    }

    "(3) schema is patched, delay between flatten calls is less than cache TTL => original schema is still cached => 1 field flattened" in {
      val propertiesCache = getCache
      val resolver = getResolver

      // first call
      flatten(propertiesCache, resolver)(
        currentTime = 1000, // ms
        schemaInRegistry = `original schema - 1 field`
      )

      // second call, 2s later, less than 10s TTL
      val result = flatten(propertiesCache, resolver)(
        currentTime = 3000, // ms
        schemaInRegistry = `patched schema - 2 fields` // different schema with the same key!
      )

      // no data from patched schema
      result must beEqualTo(List("1"))

      // Properties are cached after first call (1 second)
      propertiesCache.get((propertiesKey, 1)) must beSome

      // Properties are not cached after second call (3 seconds)
      propertiesCache.get((propertiesKey, 3)) must beNone
    }

    "(4) schema is patched, delay between flatten calls is greater than cache TTL => original schema is expired => using patched schema => 2 field flattened" in {
      val propertiesCache = getCache
      val resolver = getResolver

      // first call
      flatten(propertiesCache, resolver)(
        currentTime = 1000, // ms
        schemaInRegistry = `original schema - 1 field`
      )

      // second call, 12s later, greater than 10s TTL
      val result = flatten(propertiesCache, resolver)(
        currentTime = 13000, // ms
        schemaInRegistry = `patched schema - 2 fields` // different schema with the same key!
      )

      // Cache content expired, patched schema is fetched => 2 fields flattened
      result must beEqualTo(List("1", "2"))

      // Properties are cached after first call (1 second)
      propertiesCache.get((propertiesKey, 1)) must beSome

      // Properties are cached after second call (13 seconds)
      propertiesCache.get((propertiesKey, 13)) must beSome
    }
  }

  // Helper method to wire all test dependencies and execute EventUtils.flatten
  private def flatten(
    propertiesCache: PropertiesCache[Id],
    resolver: Resolver[Id]
  )(
    currentTime: Long,
    schemaInRegistry: Json
  ): List[String] = {

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

    val staticClock: Clock[Id] = new Clock[Id] {
      override def realTime(unit: TimeUnit): Id[Long] =
        unit.convert(currentTime, MILLISECONDS)

      override def monotonic(unit: TimeUnit): Id[Long] =
        unit.convert(currentTime * 1000000, NANOSECONDS)
    }

    val data = SelfDescribingData(schema = SchemaKey.fromUri(schemaKey).right.get, data = dataToFlatten)

    EventUtils.flatten(resolver, propertiesCache, data)(Monad[Id], testRegistryLookup, staticClock).value.right.get
  }

  private def getCache: PropertiesCache[Id] = CreateLruMap[Id, PropertiesKey, Properties].create(100)

  private def getResolver: Resolver[Id] =
    Resolver.init[Id](
      cacheSize = 10,
      cacheTtl = Some(cacheTtl),
      refs = Registry.Embedded( // not used in test as we fix returned schema in custom test RegistryLookup
        Registry.Config("Test", 0, List.empty),
        path = "/fake"
      )
    )
}
