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
package com.snowplowanalytics.snowplow.rdbloader.common.config

import io.circe._
import io.circe.generic.semiauto._

import cats.implicits._

import java.time.Instant

object Kinesis {

  sealed trait InitPosition extends Product with Serializable

  object InitPosition {
    case object Latest extends InitPosition

    case object TrimHorizon extends InitPosition

    final case class AtTimestamp(timestamp: Instant) extends InitPosition

    implicit val initPositionConfigDecoder: Decoder[InitPosition] =
      Decoder.decodeJson.emap { json =>
        json.asString match {
          case Some("TRIM_HORIZON") => InitPosition.TrimHorizon.asRight
          case Some("LATEST")       => InitPosition.Latest.asRight
          case Some(other) =>
            s"Initial position $other is unknown. Choose from LATEST and TRIM_HORIZON. AT_TIMESTAMP must provide the timestamp".asLeft
          case None =>
            val result = for {
              root <- json.asObject.map(_.toMap)
              atTimestamp <- root.get("AT_TIMESTAMP")
              atTimestampObj <- atTimestamp.asObject.map(_.toMap)
              timestampStr <- atTimestampObj.get("timestamp")
              timestamp <- timestampStr.as[Instant].toOption
            } yield InitPosition.AtTimestamp(timestamp)
            result match {
              case Some(atTimestamp) => atTimestamp.asRight
              case None =>
                "Initial position can be either LATEST or TRIM_HORIZON string or AT_TIMESTAMP object (e.g. 2020-06-03T00:00:00Z)".asLeft
            }
        }
      }
  }

  sealed trait Retrieval

  object Retrieval {
    case class Polling(maxRecords: Int) extends Retrieval

    case object FanOut extends Retrieval

    case class RetrievalRaw(`type`: String, maxRecords: Option[Int])

    implicit val retrievalRawDecoder: Decoder[RetrievalRaw] = deriveDecoder[RetrievalRaw]

    implicit val retrievalDecoder: Decoder[Retrieval] =
      Decoder.instance { cur =>
        for {
          rawParsed <- cur.as[RetrievalRaw].map(raw => raw.copy(`type` = raw.`type`.toUpperCase))
          retrieval <- rawParsed match {
                         case RetrievalRaw("POLLING", Some(maxRecords)) =>
                           Polling(maxRecords).asRight
                         case RetrievalRaw("FANOUT", _) =>
                           FanOut.asRight
                         case other =>
                           DecodingFailure(
                             s"Retrieval mode $other is not supported. Possible types are FanOut and Polling (must provide maxRecords field)",
                             cur.history
                           ).asLeft
                       }
        } yield retrieval
      }
    implicit val retrievalEncoder: Encoder[Retrieval] = deriveEncoder[Retrieval]
  }

}
