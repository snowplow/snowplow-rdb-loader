package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis

import java.net.URI

import cats.data.EitherT
import cats.implicits._

import cats.effect.{Clock, ConcurrentEffect, Concurrent, Sync}

import fs2.{Stream, Pipe}

import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data

import com.snowplowanalytics.snowplow.badrows.Processor

import com.snowplowanalytics.aws.AWSQueue

import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{Transformed, ShredderValidations}

import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sources.{Parsed, ParsedF}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks._
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.{Window, s3, file}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.generic.{Record, Partitioned}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.generic.Status.{Sealed, Closed}
import cats.effect.Temporal

object Processing {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  val Application: Processor = Processor(BuildInfo.name, BuildInfo.version)

  type Windowed[F[_], A] = Record[F, Window, A]

  def run[F[_]: ConcurrentEffect: ContextShift: Clock: Temporal](resources: Resources[F],
                                                              config: TransformerConfig.Stream): F[Unit] = {
    val transformer: Transformer[F] = config.formats match {
      case f: TransformerConfig.Formats.Shred =>
        Transformer.ShredTransformer(resources.iglu, f, resources.atomicLengths)
      case f: TransformerConfig.Formats.WideRow =>
        Transformer.WideRowTransformer(f)
    }

    val windowing: Pipe[F, ParsedF[F], Windowed[F, Parsed]] =
      Record.windowed(Window.fromNow[F](config.windowing.toMinutes.toInt))
    val onComplete: Window => F[Unit] =
      getOnComplete(config.output.compression, transformer.typesInfo, config.output.path, resources.awsQueue, resources.windows, config.featureFlags.legacyMessageFormat)
    val sinkId: Window => F[Int] =
      getSinkId(resources.windows)

    getSource[F](resources, config.input)
      .interruptWhen(resources.halt)
      .through(windowing)
      .evalTap(State.update(resources.windows))
      .through(transform[F](transformer, config.validations))
      .through(getSink[F](resources.blocker, resources.instanceId, config.output, sinkId, onComplete))
      .flatMap(_.sink)  // Sinks must be issued sequentially
      .compile
      .drain
  }

  /**
   * Get a callback that will be executed when window has been full written to destination
   * The callback sends an SQS message and modifies the global state to reflect closed window
   */
  def getOnComplete[F[_]: Sync: Clock](compression: Compression,
                                       getTypes: Set[Data.ShreddedType] => TypesInfo,
                                       root: URI,
                                       awsQueue: AWSQueue[F],
                                       state: State.Windows[F],
                                       legacyMessageFormat: Boolean)
                                      (window: Window): F[Unit] = {
    val find: State.WState => Boolean = {
      case (w, status, _) => w == window && status == Sealed
    }
    val update: State.WState => State.WState = {
      case (w, _, state) => (w, Closed, state)
    }

    state.modify(State.updateState(find, update, _._3)).flatMap { state =>
      Completion.seal[F](compression, getTypes, root, awsQueue, legacyMessageFormat)(window, state)
    } *> logger[F].debug(s"ShreddingComplete message for ${window.getDir} has been sent")
  }

  /** Auto-incrementing sink ids (file suffix) */
  def getSinkId[F[_]](windows: State.Windows[F])(window: Window): F[Int] =
    windows.modify { stack =>
      val update: State.WState => State.WState = {
        case (w, s, state) => (w, s, state.copy(sinks = state.sinks + 1))
      }
      State.updateState(_._1 == window, update, _._3.sinks)(stack)
    }

  /** Build a `Stream` of parsed Snowplow events */
  def getSource[F[_]: ConcurrentEffect: ContextShift](resources: Resources[F],
                                                      config: TransformerConfig.StreamInput): Stream[F, ParsedF[F]] =
    config match {
      case TransformerConfig.StreamInput.Kinesis(appName, streamName, region, position) =>
        sources.Kinesis.read[F](appName, streamName, region, position)
      case TransformerConfig.StreamInput.File(dir) =>
        sources.file.read[F](resources.blocker, dir)
    }

  /** Build a sink according to settings and pass it through `generic.Partitioned` */
  def getSink[F[_]: ConcurrentEffect: ContextShift](instanceId: String,
                                                    config: TransformerConfig.Output,
                                                    sinkCount: Window => F[Int],
                                                    onComplete: Window => F[Unit]): Grouping[F] =
    config match {
      case TransformerConfig.Output(path, _, _) =>
        val dataSink = Option(path.getScheme) match {
          case Some("file") =>
            file.getSink[F](blocker, path, config.compression, sinkCount) _
          case Some("s3" | "s3a" | "s3n") =>
            val (bucket, prefix) = S3.splitS3Path(S3.Folder.coerce(path.toString))
            s3.getSink[F](bucket, prefix, config.compression, sinkCount, instanceId) _
          case _ =>
            val error = new IllegalArgumentException(s"Cannot create sink for $path. Possible options are file:// and s3://")
            (_: Window) => (_: Transformed.Path) =>
              (_: Stream[F, Transformed.Data]) =>
                Stream.raiseError[F](error)
        }

        Partitioned.write[F, Window, Transformed.Path, Transformed.Data](dataSink, onComplete)
    }

  def transform[F[_]: Concurrent: Clock: Temporal](transformer: Transformer[F],
                                                validations: TransformerConfig.Validations): Pipe[F, Windowed[F, Parsed], Windowed[F, (Transformed.Path, Transformed.Data)]] = {
    _.flatMap { record =>
      val shreddedRecord = record.traverse { parsed =>
        val res = for {
          event       <- EitherT.fromEither[F](parsed)
          _           <- EitherT.fromEither[F](ShredderValidations(Application, event, validations).toLeft(()))
          transformed <- transformer.goodTransform(event)
        } yield transformed
        res.leftMap(transformer.badTransform).value
      }
      Stream.eval(shreddedRecord).flatMap {
        case Record.Data(window, checkpoint, Right(shredded)) =>
          Record.mapWithLast(shredded)(s => Record.Data(window, None, s.split), s => Record.Data(window, checkpoint, s.split))
        case Record.Data(window, checkpoint, Left(badRow)) =>
          Stream.emit(Record.Data(window, checkpoint, badRow.split))
        case Record.EndWindow(window, next, checkpoint) =>
          Stream.emit(Record.EndWindow[F, Window, (Transformed.Path, Transformed.Data)](window, next, checkpoint))
      }
    }
  }
}
