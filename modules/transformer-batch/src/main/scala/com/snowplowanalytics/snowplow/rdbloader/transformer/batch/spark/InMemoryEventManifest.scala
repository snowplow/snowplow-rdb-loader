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

package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark

import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifest
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark.InMemoryEventManifest.DuplicateItem

import java.time.Instant
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

/**
 * In memory event manifest for testing purposes. It stores items in an array.
 */
class InMemoryEventManifest extends EventsManifest {

  var events: ArrayBuffer[InMemoryEventManifest.DuplicateItem] = ArrayBuffer.empty

  /**
   * This function mimics actual behavior used in DynamoDbManifest
   * https://github.com/snowplow-incubator/snowplow-events-manifest/blob/master/src/main/scala/com/snowplowanalytics/snowplow/eventsmanifest/DynamoDbManifest.scala#L71
   */
  override def put(eventId: UUID, eventFingerprint: String, etlTstamp: Instant): Boolean = {
    val duplicateItem = DuplicateItem(eventId: UUID, eventFingerprint: String, etlTstamp: Instant)
    val isEventUnique = !events.exists(e => e.eventId == eventId && e.eventFingerprint == eventFingerprint)
    val timestampExists = events.exists(_.etlTstamp == etlTstamp)
    if (isEventUnique) {
      events.append(duplicateItem)
    }
    isEventUnique || timestampExists
  }

  def deleteEvents(): Unit = events = ArrayBuffer.empty

  def getEventIds: List[String] = events.map(_.eventId.toString).toList
}

object InMemoryEventManifest {
  case class DuplicateItem(eventId: UUID, eventFingerprint: String, etlTstamp: Instant)
}
