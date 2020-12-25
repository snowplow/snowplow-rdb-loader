/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.shredder

import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifest
import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload}
import java.time.Instant
import java.util.UUID

import scala.util.control.NonFatal

import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException
import io.circe.literal._

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.Contexts

import com.snowplowanalytics.snowplow.shredder.transformation.EventUtils.BadRowsProcessor

import com.snowplowanalytics.iglu.core.{SchemaVer, SelfDescribingData, SchemaKey}

object Deduplication {

  val DuplicateSchema = SchemaKey("com.snowplowanalytics.snowplow", "duplicate", "jsonschema", SchemaVer.Full(1,0,0))

  /** Add duplicate context */
  def withSynthetic(event: Event): Event = {
    val newContext = SelfDescribingData(DuplicateSchema, json"""{"originalEventId":${event.event_id}}""")
    val updatedContexts = newContext :: event.derived_contexts.data
    val newEventId = UUID.randomUUID()
    event.copy(event_id = newEventId, derived_contexts = Contexts(updatedContexts))
  }

  /**
   * Try to store event components in duplicate storage and check if it was stored before
   * If event is unique in storage - true will be returned,
   * If event is already in storage, with different etlTstamp - false will be returned,
   * If event is already in storage, but with same etlTstamp - true will be returned (previous shredding was interrupted),
   * If storage is not configured - true will be returned.
   * If provisioned throughput exception happened - interrupt whole job
   * If other runtime exception happened - failure is returned to be used as bad row
   * @param event whole enriched event with possibly faked fingerprint
   * @param tstamp the ETL tstamp, an earliest timestamp in a batch
   * @param duplicateStorage object dealing with possible duplicates
   * @return boolean inside validation, denoting presence or absence of event in storage
   */
  @throws[RuntimeException]
  def crossBatch(event: Event, tstamp: Instant, duplicateStorage: Option[EventsManifest]): Either[BadRow, Boolean] = {
    (event, duplicateStorage) match {
      case (_, Some(storage)) =>
        try {
          Right(storage.put(event.event_id, event.event_fingerprint.getOrElse(UUID.randomUUID().toString), tstamp))
        } catch {
          case e: ProvisionedThroughputExceededException =>
            throw e
          case NonFatal(e) =>
            val payload = Payload.LoaderPayload(event)
            Left(BadRow.LoaderRuntimeError(BadRowsProcessor, Option(e.getMessage).getOrElse(e.toString), payload))
        }
      case _ => Right(true)
    }
  }
}
