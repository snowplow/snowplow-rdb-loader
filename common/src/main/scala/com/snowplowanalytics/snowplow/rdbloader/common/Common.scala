package com.snowplowanalytics.snowplow.rdbloader.common

import io.circe.Json

import cats.Monad
import cats.data.EitherT
import cats.syntax.either._
import cats.effect.Clock

import com.snowplowanalytics.iglu.core._
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.client.ClientError.ResolutionError

import com.snowplowanalytics.iglu.schemaddl.IgluSchema
import com.snowplowanalytics.iglu.schemaddl.migrations.Migration.OrderedSchemas
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._

object Common {
  /**
    * Error specific to shredding JSON instance into tabular format
    * `SchemaList` is unavailable (in case no Iglu Server hosts this schemas)
    * Particular schema could not be fetched, thus whole flattening algorithm cannot be built
    */
  sealed trait FlatteningError
  object FlatteningError {
    case class SchemaListResolution(error: ResolutionError) extends FlatteningError
    case class SchemaResolution(error: ResolutionError) extends FlatteningError
    case class Parsing(error: String) extends FlatteningError
  }

  // Cache = Map[SchemaKey, OrderedSchemas]

  def getOrdered[F[_]: Monad: RegistryLookup: Clock](resolver: Resolver[F], key: SchemaKey): EitherT[F, FlatteningError, OrderedSchemas] =
    getOrdered(resolver, key.vendor, key.name, key.version.model)

  def getOrdered[F[_]: Monad: RegistryLookup: Clock](resolver: Resolver[F], vendor: String, name: String, model: Int): EitherT[F, FlatteningError, OrderedSchemas] =
    for {
      schemaList <- EitherT[F, ResolutionError, SchemaList](resolver.listSchemas(vendor, name, Some(model))).leftMap(FlatteningError.SchemaListResolution)
      ordered <- OrderedSchemas.fromSchemaList(schemaList, fetch(resolver))
    } yield ordered

  def fetch[F[_]: Monad: RegistryLookup: Clock](resolver: Resolver[F])(key: SchemaKey): EitherT[F, FlatteningError, IgluSchema] =
    for {
      json <- EitherT(resolver.lookupSchema(key, 2)).leftMap(FlatteningError.SchemaResolution)
      schema <- EitherT.fromEither(parseSchema(json))
    } yield schema

  /** Parse JSON into self-describing schema, or return `FlatteningError` */
  private def parseSchema(json: Json): Either[FlatteningError, IgluSchema] =
    for {
      selfDescribing <- SelfDescribingSchema.parse(json).leftMap(code => FlatteningError.Parsing(s"Cannot parse ${json.noSpaces} payload as self-describing schema, ${code.code}"))
      parsed <- Schema.parse(selfDescribing.schema).toRight(FlatteningError.Parsing(s"Cannot parse ${selfDescribing.self.schemaKey.toSchemaUri} payload as JSON Schema"))
    } yield SelfDescribingSchema(selfDescribing.self, parsed)


  import java.util.UUID
  import java.time.{Instant}
  import io.circe.literal._
  import com.snowplowanalytics.snowplow.analytics.scalasdk.Event


  case class Hierarchy(eventId: UUID, collectorTstamp: Instant, entity: SelfDescribingData[Json]) {
    def dumpJson: String = json"""
      {
        "schema": {
          "vendor": ${entity.schema.vendor},
          "name": ${entity.schema.name},
          "format": ${entity.schema.format},
          "version": ${entity.schema.version.asString}
        },
        "data": ${entity.data},
        "hierarchy": {
          "rootId": $eventId,
          "rootTstamp": ${collectorTstamp.formatted},
          "refRoot": "events",
          "refTree": ["events", ${entity.schema.name}],
          "refParent":"events"
        }
      }""".noSpaces
  }

  def getEntities(event: Event): List[SelfDescribingData[Json]] =
    event.unstruct_event.data.toList ++
      event.derived_contexts.data ++
      event.contexts.data

  def getShreddedEntities(event: Event): List[Hierarchy] =
    getEntities(event).map(json => Hierarchy(event.event_id, event.collector_tstamp, json))
}
