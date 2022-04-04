package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sources

import java.nio.file.Paths

import cats.Applicative
import cats.syntax.either._

import cats.effect.{Concurrent, Sync}

import fs2.Stream
import fs2.io.file.{readAll, directoryStream}
import fs2.text.{utf8Decode, lines}

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{ BadRow, Payload => BRPayload }

import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.Processing.Application

import org.typelevel.log4cats.slf4j.Slf4jLogger

object file {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def read[F[_]: Concurrent: ContextShift](dirPath: String): Stream[F, ParsedF[F]] =
    directoryStream(blocker, Paths.get(dirPath))
      .flatMap { filePath =>
        Stream.eval_(logger.debug(s"Reading $filePath")) ++
          readAll(filePath, blocker, 4096).through(utf8Decode).through(lines)
      }
      .map(line => (parse(line), Applicative[F].unit))

  def parse(line: String): Parsed =
    Event.parse(line).toEither.leftMap { error =>
      BadRow.LoaderParsingError(Application, error, BRPayload.RawPayload(line))
    }
}
