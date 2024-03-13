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

import java.time.Instant

import cats.syntax.list._

import io.circe.syntax._

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.badrows.Failure.GenericFailure
import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload, Processor}

import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Validations

object ShredderValidations {

  def apply(
    processor: Processor,
    event: Event,
    validations: Validations
  ): Option[BadRow] =
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
    ).map((checkTimestampHelper(minimumTimestamp) _).tupled).flatten

  private def checkTimestampHelper(minimumTimestamp: Instant)(timestamp: Option[Instant], col: String): Option[String] =
    for {
      ts <- timestamp
      if ts.isBefore(minimumTimestamp)
    } yield s"Timestamp ${ts.toString} is before than minimum timestamp, at column $col"
}
