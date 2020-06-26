/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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

import com.snowplowanalytics.iglu.core.SchemaKey

import Decoder._
import Entities._

trait Decoder[A] {
  def decode(resultSet: ResultSet): Either[JdbcDecodeError, A]
  def name: String
}

object Decoder {

  final case class JdbcDecodeError(message: String)

  implicit val countDecoder: Decoder[Option[Count]] =
    new Decoder[Option[Count]] {
      def name = "Option[Count]"
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
      def name = "Option[Timestamp]"
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
      def name = "Option[LoadManifestItem]"
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

  implicit val booleanDecoder: Decoder[Boolean] =
    new Decoder[Boolean] {
      def name = "Boolean"
      final def decode(resultSet: ResultSet): Either[JdbcDecodeError, Boolean] = {
        var buffer: Boolean = false
        try {
          if (resultSet.next()) {
            val bool = resultSet.getBoolean(1)
            buffer = bool
            buffer.asRight[JdbcDecodeError]
          } else false.asRight[JdbcDecodeError]
        } catch {
          case s: SQLException =>
            JdbcDecodeError(s.getMessage).asLeft[Boolean]
        } finally {
          resultSet.close()
        }
      }
    }

  implicit val columnsDecoder: Decoder[Columns] =
    new Decoder[Columns] {
      def name = "Columns"
      final def decode(resultSet: ResultSet): Either[JdbcDecodeError, Columns] = {
        val buffer = collection.mutable.ListBuffer.empty[String]
        try {
          while (resultSet.next()) {
            val col = resultSet.getString("column")
            buffer.append(col)
          }
          Columns(buffer.toList).asRight
        } catch {
          case s: SQLException =>
            JdbcDecodeError(s.getMessage).asLeft[Columns]
        } finally {
          resultSet.close()
        }
      }
    }

  implicit val tableStateDecoder: Decoder[TableState] =
    new Decoder[TableState] {
      def name = "TableState"
      final def decode(resultSet: ResultSet): Either[JdbcDecodeError, TableState] = {
        try {
          if (resultSet.next()) {
            val col = SchemaKey.fromUri(resultSet.getString(1))
            col.map(x => TableState(x)).leftMap(e => JdbcDecodeError(s"Table comment is not valid SchemaKey, ${e.code}"))
          } else JdbcDecodeError("Table description is not available").asLeft
        } catch {
          case s: SQLException =>
            JdbcDecodeError(s.getMessage).asLeft[TableState]
        } finally {
          resultSet.close()
        }
      }
    }

}
