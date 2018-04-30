/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader
package db

import java.sql.{ResultSet, SQLException}

import cats.implicits._

import Decoder._
import Entities._

trait Decoder[A] {
  def decode(resultSet: ResultSet): Either[JdbcDecodeError, A]
}

object Decoder {

  final case class JdbcDecodeError(message: String)

  implicit val countDecoder: Decoder[Option[Count]] =
    new Decoder[Option[Count]] {
      final def decode(resultSet: ResultSet): Either[JdbcDecodeError, Option[Count]] = {
        var buffer: Count = null
        try {
          if (resultSet.next()) {
            val count = resultSet.getLong("count")
            buffer = Count(count)
            Option(buffer).asRight[JdbcDecodeError]
          } else None.asRight[JdbcDecodeError]
        } catch {
          case s: SQLException =>
            JdbcDecodeError(s.getMessage).asLeft[Option[Count]]
        } finally {
          resultSet.close()
        }
      }
    }

  implicit val timestampDecoder: Decoder[Option[Timestamp]] =
    new Decoder[Option[Timestamp]] {
      final def decode(resultSet: ResultSet): Either[JdbcDecodeError, Option[Timestamp]] = {
        var buffer: Timestamp = null
        try {
          if (resultSet.next()) {
            val etlTstamp = resultSet.getTimestamp("etl_tstamp")
            buffer = Timestamp(etlTstamp)
            Option(buffer).asRight[JdbcDecodeError]
          } else None.asRight[JdbcDecodeError]
        } catch {
          case s: SQLException =>
            JdbcDecodeError(s.getMessage).asLeft[Option[Timestamp]]
        } finally {
          resultSet.close()
        }
      }
    }

  implicit val manifestItemDecoder: Decoder[Option[LoadManifestItem]] =
    new Decoder[Option[LoadManifestItem]] {
      final def decode(resultSet: ResultSet): Either[JdbcDecodeError, Option[LoadManifestItem]] = {
        var buffer: LoadManifestItem = null
        try {
          if (resultSet.next()) {
            val etlTstamp = resultSet.getTimestamp("etl_tstamp")
            val commitTstamp = resultSet.getTimestamp("commit_tstamp")
            val eventCount = resultSet.getInt("event_count")
            val shreddedCardinality = resultSet.getInt("shredded_cardinality")

            eventCount.toString ++ shreddedCardinality.toString // forcing NPE

            buffer = LoadManifestItem(etlTstamp, commitTstamp, eventCount, shreddedCardinality)

            Option(buffer).asRight[JdbcDecodeError]
          } else None.asRight[JdbcDecodeError]
        } catch {
          case s: SQLException =>
            JdbcDecodeError(s.getMessage).asLeft[Option[LoadManifestItem]]
          case _: NullPointerException =>
            val message = "Error while decoding Load Manifest Item. Not all expected values are available"
            JdbcDecodeError(message).asLeft[Option[LoadManifestItem]]
        } finally {
          resultSet.close()
        }
      }
    }
}
