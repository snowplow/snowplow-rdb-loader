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
