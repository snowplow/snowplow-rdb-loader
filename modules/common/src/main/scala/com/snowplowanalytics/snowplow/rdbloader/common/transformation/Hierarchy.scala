/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.common.transformation

import java.util.UUID
import java.time.Instant

import io.circe.{Encoder, Json}
import io.circe.literal._
import io.circe.syntax._

import com.snowplowanalytics.iglu.core.SelfDescribingData

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

case class Hierarchy(
  eventId: UUID,
  collectorTstamp: Instant,
  entity: SelfDescribingData[Json]
) { self =>
  def dumpJson: String = self.asJson.noSpaces
}

object Hierarchy {
  implicit val hierarchyCirceEncoder: Encoder[Hierarchy] =
    Encoder.instance { h =>
      json"""{
          "schema": {
            "vendor": ${h.entity.schema.vendor},
            "name": ${h.entity.schema.name},
            "format": ${h.entity.schema.format},
            "version": ${h.entity.schema.version.asString}
          },
          "data": ${h.entity.data},
          "hierarchy": {
            "rootId": ${h.eventId},
            "rootTstamp": ${h.collectorTstamp.formatted},
            "refRoot": "events",
            "refTree": ["events", ${h.entity.schema.name}],
            "refParent":"events"
          }
        }"""
    }

  def fromEvent(event: Event): List[Hierarchy] =
    EventUtils.getEntities(event).map(json => Hierarchy(event.event_id, event.collector_tstamp, json))
}
