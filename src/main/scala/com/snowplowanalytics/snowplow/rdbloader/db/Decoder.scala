package com.snowplowanalytics.snowplow.rdbloader.db

import cats.implicits._
import java.sql.{ResultSet, SQLException}

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
          resultSet.next()
          val count = resultSet.getLong("count")
          buffer = Count(count)
          Option(buffer).asRight[JdbcDecodeError]
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
          resultSet.next()
          val etlTstamp = resultSet.getTimestamp("etl_tstamp")
          buffer = Timestamp(etlTstamp.toInstant)
          Option(buffer).asRight[JdbcDecodeError]
        } catch {
          case s: SQLException =>
            JdbcDecodeError(s.getMessage).asLeft[Option[Timestamp]]
        } finally {
          resultSet.close()
        }
      }
    }

  implicit val manifestItemsDecoder: Decoder[List[LoadManifestItem]] =
    new Decoder[List[LoadManifestItem]] {
      final def decode(resultSet: ResultSet): Either[JdbcDecodeError, List[LoadManifestItem]] = {
        var buffer = collection.mutable.ListBuffer.newBuilder[LoadManifestItem]
        try {
          while (resultSet.next()) {
            val etlTstamp = resultSet.getTimestamp("etl_tstamp").toInstant
            val commitTstamp = resultSet.getTimestamp("commit_tstamp").toInstant
            val eventCount = resultSet.getInt("event_count")
            val shreddedCardinality = resultSet.getInt("shredded_cardinality")
            buffer += LoadManifestItem(etlTstamp, commitTstamp, eventCount, shreddedCardinality)
          }
          buffer.result().toList.asRight[JdbcDecodeError]
        } catch {
          case s: SQLException =>
            JdbcDecodeError(s.getMessage).asLeft[List[LoadManifestItem]]
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
          resultSet.next()
          val etlTstamp = resultSet.getTimestamp("etl_tstamp").toInstant
          val commitTstamp = resultSet.getTimestamp("commit_tstamp").toInstant
          val eventCount = resultSet.getInt("event_count")
          val shreddedCardinality = resultSet.getInt("shredded_cardinality")

          buffer = LoadManifestItem(etlTstamp, commitTstamp, eventCount, shreddedCardinality)

          Option(buffer).asRight[JdbcDecodeError]
        } catch {
          case s: SQLException =>
            JdbcDecodeError(s.getMessage).asLeft[Option[LoadManifestItem]]
        } finally {
          resultSet.close()
        }
      }
    }
}
