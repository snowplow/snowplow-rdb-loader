/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.common.transformation

import java.time.Instant

import cats.syntax.list._

import io.circe.syntax._

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.badrows.Failure.GenericFailure
import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload, Processor}

import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Validations

object ShredderValidations {

  def apply(processor: Processor, event: Event, validations: Validations): Option[BadRow] =
    for {
      minimumTimestamp <- validations.minimumTimestamp
      failures <- checkTimestamp(event, minimumTimestamp).toNel
    } yield BadRow.GenericError(processor, GenericFailure(Instant.now, failures), Payload.RawPayload(event.asJson.noSpaces))


  private def checkTimestamp(event: Event, minimumTimestamp: Instant): List[String] =
    List(
      (Some(event.collector_tstamp), "collector_tstamp"),
      (event.derived_tstamp, "derived_tstamp"),
      (event.dvce_created_tstamp, "dvce_created_tstamp"),
      (event.dvce_sent_tstamp, "dvce_sent_tstamp"),
      (event.etl_tstamp, "etl_tstamp"),
      (event.refr_dvce_tstamp, "refr_dvce_tstamp"),
      (event.true_tstamp, "true_tstamp")
    ).map((checkTimestampHelper(minimumTimestamp) _).tupled)
    .flatten

  private def checkTimestampHelper(minimumTimestamp: Instant)(timestamp: Option[Instant], col: String): Option[String] =
    for {
      ts <- timestamp
      if ts.isBefore(minimumTimestamp)
    } yield s"Timestamp ${ts.toString} is before than minimum timestamp, at column $col"
}
