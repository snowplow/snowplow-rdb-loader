/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.shredder.transformation

import java.util.UUID
import java.time.Instant

import io.circe.{Encoder, Json}
import io.circe.literal._
import io.circe.syntax._

import com.snowplowanalytics.iglu.core.SelfDescribingData

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.shredder.transformation.EventUtils.getEntities

case class Hierarchy(eventId: UUID, collectorTstamp: Instant, entity: SelfDescribingData[Json]) { self =>
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
    getEntities(event).map(json => Hierarchy(event.event_id, event.collector_tstamp, json))
}

