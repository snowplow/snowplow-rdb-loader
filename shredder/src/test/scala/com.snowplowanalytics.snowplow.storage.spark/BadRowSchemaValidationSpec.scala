package com.snowplowanalytics.snowplow.storage.spark

import java.time.Instant
import java.util.UUID

import cats.data.NonEmptyList
import io.circe.literal._
import io.circe.parser.parse
import com.snowplowanalytics.iglu.client.ClientError.{ValidationError => IgluValidationError}
import com.snowplowanalytics.iglu.client.validator.ValidatorError.InvalidData
import com.snowplowanalytics.iglu.client.validator.ValidatorReport
import com.snowplowanalytics.iglu.client.{CirceValidator, Resolver}
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent}
import com.snowplowanalytics.snowplow.storage.spark.BadRow.{DeduplicationError, SchemaError, ShreddingError, ValidationError}
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import org.specs2.{ScalaCheck, Specification}

object BadRowSchemaValidationSpec {

  val resolverConfig = json"""
      {
         "schema":"iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
         "data":{
            "cacheSize":500,
            "repositories":[
              {
               "name":"Iglu Central Mirror",
               "priority":0,
               "vendorPrefixes":[
                 "com.snowplowanalytics"
               ],
               "connection":{
                 "http":{
                  "uri":"http://18.195.229.140"
                 }
               }
              },
              {
               "name":"Local Iglu Central",
               "priority":0,
               "vendorPrefixes":[
                 "com.snowplowanalytics"
               ],
               "connection":{
                 "http":{
                  "uri":"http://localhost:4040"
                 }
               }
              },
              {
               "name":"Iglu Central",
               "priority":0,
               "vendorPrefixes":[
                 "com.snowplowanalytics"
               ],
               "connection":{
                 "http":{
                   "uri":"http://iglucentral.com"
                 }
               }
              }
            ]
         }
      }
  """

  val resolver = Resolver.parse(resolverConfig).fold(e => throw new RuntimeException(e.toString), identity)

  def minimalEvent(id: UUID, collectorTstamp: Instant, vCollector: String, vTstamp: String): Event =
    Event(None, None, None, collectorTstamp, None, None, id, None, None, None, vCollector, vTstamp, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      Contexts(Nil), None, None, None, None, None, UnstructEvent(None), None, None, None, None, None, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      Contexts(Nil), None, None, None, None, None, None, None, None)

  val exampleMinimalEvent = minimalEvent(
    UUID.fromString("ba553b7f-63d5-47ad-8697-06016b472c34"),
    Instant.ofEpochMilli(1550477167580L),
    "bq-loader-test",
    "bq-loader-test"
  )

  val shreddingErrorGen = for {
    original <- Gen.alphaNumStr
    errors <- Gen.nonEmptyListOf(Gen.alphaNumStr)
  } yield ShreddingError(original, NonEmptyList.fromListUnsafe(errors))

  val schemaVerGen = for {
    model <- Gen.chooseNum(0, 9)
    revision <- Gen.chooseNum(0, 9)
    addition <- Gen.chooseNum(0, 9)
  } yield SchemaVer.Full(model, revision, addition)

  val schemaKeyGen = for {
    vendor <- Gen.identifier
    name <- Gen.identifier
    format <- Gen.identifier
    version <- schemaVerGen
  } yield SchemaKey(vendor, name, format, version)

  val schemaErrorGen = for {
    schemaKey <- schemaKeyGen
  } yield SchemaError(
    schemaKey,
    IgluValidationError(InvalidData(NonEmptyList.of(ValidatorReport("message", None, List(), None))))
  )

  val validationErrorGen = for {
    schemaErrors <- Gen.nonEmptyListOf(schemaErrorGen)
  } yield ValidationError(exampleMinimalEvent, NonEmptyList.fromListUnsafe(schemaErrors))

  val deduplicationErrorGen = for {
    error <- Gen.alphaNumStr
  } yield DeduplicationError(exampleMinimalEvent, error)

  def validateBadRow(badRow: BadRow, schemaKey: SchemaKey) = {
    val schema = resolver.lookupSchema(schemaKey, 2)
    CirceValidator.validate(
      parse(badRow.toCompactJson).right.getOrElse(throw new RuntimeException(s"Error while parsing to json: ${badRow.toCompactJson}")),
      schema.right.getOrElse(throw new RuntimeException(s"Schema could not be found: $schema")))
  }
}

class BadRowSchemaValidationSpec extends Specification with ScalaCheck { def is = s2"""
  json of 'shredding error' complies its schema $e1
  json of 'validation error' complies its schema $e2
  json of 'deduplication error' complies its schema $e3
  """
  import BadRowSchemaValidationSpec._

  def e1 = {
    forAll(shreddingErrorGen) {
      shreddingError => validateBadRow(shreddingError, BadRowSchemas.ShreddingError) must beRight
    }
  }

  def e2 = {
    forAll(validationErrorGen) {
      validationError => validateBadRow(validationError, BadRowSchemas.ValidationError) must beRight
    }
  }

  def e3 = {
    forAll(deduplicationErrorGen) {
      deduplicationError => validateBadRow(deduplicationError, BadRowSchemas.DeduplicationError) must beRight
    }
  }
}
