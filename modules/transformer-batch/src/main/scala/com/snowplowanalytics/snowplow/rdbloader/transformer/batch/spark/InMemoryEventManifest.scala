/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */

package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark

import java.time.Instant
import java.util.UUID

import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifest

/**
 * In memory event manifest for testing purposes. It stores items in an array.
 */
class InMemoryEventManifest extends EventsManifest {

  var events: Map[(UUID, String), Long] = Map.empty

  /**
   * This function mimics actual behavior used in DynamoDbManifest
   * https://github.com/snowplow-incubator/snowplow-events-manifest/blob/master/src/main/scala/com/snowplowanalytics/snowplow/eventsmanifest/DynamoDbManifest.scala#L71
   */
  override def put(
    eventId: UUID,
    eventFingerprint: String,
    etlTstamp: Instant
  ): Boolean = {
    val cond = events.get((eventId, eventFingerprint)).forall(_ == etlTstamp.getEpochSecond)
    if (cond) {
      events = events.updated((eventId, eventFingerprint), etlTstamp.getEpochSecond)
    }
    cond
  }

  def deleteEvents(): Unit = events = Map.empty

  def getEventIds: List[String] = events.toList.map(_._1._1.toString)
}
