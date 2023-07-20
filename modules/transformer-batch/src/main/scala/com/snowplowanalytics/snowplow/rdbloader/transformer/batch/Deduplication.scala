/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import java.time.Instant
import java.util.UUID

import scala.util.control.NonFatal

import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException

import io.circe.literal._

import org.apache.spark.rdd.RDD

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.Contexts

import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifest
import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload}

import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.Config.{Deduplication => DedupConfig}

object Deduplication {

  /** Arbitrary UUID, used as a dummy event id for all bad rows */
  val BadRowId = UUID.randomUUID()

  val DuplicateSchema = SchemaKey("com.snowplowanalytics.snowplow", "duplicate", "jsonschema", SchemaVer.Full(1, 0, 0))

  /** Add duplicate context */
  def withSynthetic(event: Event): Event = {
    val newContext = SelfDescribingData(DuplicateSchema, json"""{"originalEventId":${event.event_id}}""")
    val updatedContexts = newContext :: event.derived_contexts.data
    val newEventId = UUID.randomUUID()
    event.copy(event_id = newEventId, derived_contexts = Contexts(updatedContexts))
  }

  /**
   * Deduplication result, containing either original dataset and set of synthetic deduplicate UUIDs
   * (in case of broadcast deduplication) or deduplicated dataset and empty set of UUIDs (in case of
   * JOIN deduplication) for later id generation original dataset and empty UUID set (in case of
   * disabled deduplication) - no-op basically
   *
   * @param events
   *   parsed dataset of events, can be original or deduplicated
   * @param duplicates
   *   set of duplicate UUIDs, can be empty or non-empty
   */
  final case class Result(events: RDD[Either[BadRow, Event]], duplicates: Set[UUID])

  /** Run a deduplication process against a dataset */
  def sytheticDeduplication(config: DedupConfig, events: RDD[Either[BadRow, Event]]): Result = {
    val cardinality = config.synthetic match {
      case DedupConfig.Synthetic.Broadcast(c) => c
      case _ => 1
    }
    // Count synthetic duplicates, defined as events with the same id but different fingerprints
    // Won't be executed in case of None
    val duplicates = events
      .flatMap {
        case Right(e) => Some((e.event_id, 1L))
        case Left(_) => None
      }
      .reduceByKey(_ + _)
      .flatMap {
        case (id, count) if count > cardinality => Some(id)
        case _ => None
      }

    config.synthetic match {
      case DedupConfig.Synthetic.None =>
        Result(events, Set.empty)
      case DedupConfig.Synthetic.Join =>
        val rdd = events
          .map {
            case Right(event) => event.event_id -> Right(event)
            case Left(badRow) => BadRowId -> Left(badRow)
          }
          .leftOuterJoin(duplicates.map((id: UUID) => (id, ())))
          .map {
            case (_, (Right(event), d)) if d.isDefined => Right(withSynthetic(event))
            case (_, (Right(event), _)) => Right(event)
            case (_, (Left(badRow), _)) => Left(badRow)
          }
        Result(rdd, Set.empty)
      case DedupConfig.Synthetic.Broadcast(_) =>
        Result(events, duplicates.collect().toSet)
    }
  }

  /**
   * Try to store event components in duplicate storage and check if it was stored before If event
   * is unique in storage - true will be returned, If event is already in storage, with different
   * etlTstamp - false will be returned, If event is already in storage, but with same etlTstamp -
   * true will be returned (previous shredding was interrupted), If storage is not configured - true
   * will be returned. If provisioned throughput exception happened - interrupt whole job If other
   * runtime exception happened - failure is returned to be used as bad row
   * @param event
   *   whole enriched event with possibly faked fingerprint
   * @param tstamp
   *   the ETL tstamp, an earliest timestamp in a batch
   * @param duplicateStorage
   *   object dealing with possible duplicates
   * @return
   *   boolean inside validation, denoting presence or absence of event in storage
   */
  @throws[RuntimeException]
  def crossBatch(
    event: Event,
    tstamp: Instant,
    duplicateStorage: Option[EventsManifest]
  ): Either[BadRow, Boolean] =
    (event, duplicateStorage) match {
      case (_, Some(storage)) =>
        try
          Right(storage.put(event.event_id, event.event_fingerprint.getOrElse(UUID.randomUUID().toString), tstamp))
        catch {
          case e: ProvisionedThroughputExceededException =>
            throw e
          case NonFatal(e) =>
            val payload = Payload.LoaderPayload(event)
            Left(BadRow.LoaderRuntimeError(ShredJob.BadRowsProcessor, Option(e.getMessage).getOrElse(e.toString), payload))
        }
      case _ => Right(true)
    }
}
