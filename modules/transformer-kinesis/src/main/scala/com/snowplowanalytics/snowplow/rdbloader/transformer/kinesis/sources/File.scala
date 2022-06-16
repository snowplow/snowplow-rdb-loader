package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sources

import java.nio.file.Paths

import cats.syntax.either._

import cats.effect.{Concurrent, Blocker, Sync, ContextShift}

import fs2.Stream
import fs2.io.file.{readAll, directoryStream}
import fs2.text.{utf8Decode, lines}

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{ BadRow, Payload => BRPayload }

import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.Processing.Application
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.Config.StreamInput.{File => FileConfig}

import org.typelevel.log4cats.slf4j.Slf4jLogger

object file {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def read[F[_]: Concurrent: ContextShift](blocker: Blocker, config: FileConfig): Stream[F, ParsedC[Unit]] =
    directoryStream(blocker, Paths.get(config.dir))
      .flatMap { filePath =>
        Stream.eval_(logger.debug(s"Reading $filePath")) ++
          readAll(filePath, blocker, 4096).through(utf8Decode).through(lines)
      }
      .map(line => (parse(line), ()))

  def parse(line: String): Parsed =
    Event.parse(line).toEither.leftMap { error =>
      BadRow.LoaderParsingError(Application, error, BRPayload.RawPayload(line))
    }
}
