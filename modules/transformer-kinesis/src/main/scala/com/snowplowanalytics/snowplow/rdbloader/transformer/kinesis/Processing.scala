package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis

import cats.data.EitherT
import cats.effect.{Blocker, Clock, Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.snowplowanalytics.aws.AWSQueue
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Data, Event}
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{ShredderValidations, Transformed}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks._
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.generic.Status.{Closed, Sealed}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.generic.{Partitioned, Record}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sources.{Parsed, ParsedF}
import fs2.{Pipe, Stream}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.net.URI

object Processing {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  val Application: Processor = Processor(BuildInfo.name, BuildInfo.version)

  final case class SuccessfulTransformation(original: Event, output: List[Transformed])
  
  type Windowed[F[_], A] = Record[F, Window, A]
  type TransformationResult = Either[BadRow, SuccessfulTransformation]

  def run[F[_]: ConcurrentEffect: ContextShift: Clock: Timer](resources: Resources[F],
                                                              config: TransformerConfig.Stream): F[Unit] = {
    val windowing: Pipe[F, ParsedF[F], Windowed[F, Parsed]] =
      Record.windowed(Window.fromNow[F](config.windowing.toMinutes.toInt))

    val streamOfWindowedRecords = getSource[F](resources, config.input).through(windowing)

    runWindowed(streamOfWindowedRecords, resources, config)
  }

  def runWindowed[F[_]: ConcurrentEffect: ContextShift: Clock: Timer](windowedRecords: Stream[F, Windowed[F, Parsed]],
                                                                      resources: Resources[F],
                                                                      config: TransformerConfig.Stream): F[Unit] = {
    val transformer: Transformer[F] = config.formats match {
      case f: TransformerConfig.Formats.Shred =>
        Transformer.ShredTransformer(resources.iglu, f, resources.atomicLengths)
      case f: TransformerConfig.Formats.WideRow =>
        Transformer.WideRowTransformer(f)
    }

    val onComplete: Window => F[Unit] =
      getOnComplete(config.output.compression, transformer.typesInfo, config.output.path, resources.awsQueue, resources.windows, config.featureFlags.legacyMessageFormat)
    val sinkId: Window => F[Int] =
      getSinkId(resources.windows)

    windowedRecords
      .interruptWhen(resources.halt)
      .through(attemptTransform(transformer, config.validations))
      .evalTap(State.update(resources.windows))
      .through(handleTransformResult(transformer))
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
  def getSink[F[_]: ConcurrentEffect: ContextShift](blocker: Blocker,
                                                    instanceId: String,
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
            (_: Window) => (_: SinkPath) =>
              (_: Stream[F, Transformed.Data]) =>
                Stream.raiseError[F](error)
        }

        Partitioned.write[F, Window, SinkPath, Transformed.Data](dataSink, onComplete)
    }

  def attemptTransform[F[_]: Concurrent: Clock: Timer](transformer: Transformer[F],
                                                       validations: TransformerConfig.Validations): Pipe[F, Windowed[F, Parsed], Windowed[F, TransformationResult]] = {
    _.evalMap { record =>
      record.traverse { parsed =>
        (for {
          event       <- EitherT.fromEither[F](parsed)
          _           <- EitherT.fromEither[F](ShredderValidations(Application, event, validations).toLeft(()))
          transformed <- transformer.goodTransform(event)
        } yield SuccessfulTransformation(original = event, output = transformed)).value
      }
    }
  }

  def handleTransformResult[F[_]](transformer: Transformer[F]): Pipe[F, Windowed[F, TransformationResult], Windowed[F, (SinkPath, Transformed.Data)]] = {
    _.flatMap { record =>
      record.map(_.leftMap(transformer.badTransform)) match {
        case Record.Data(window, checkpoint, Right(shredded)) =>
          Record.mapWithLast(shredded.output)(s => Record.Data(window, None, s.split), s => Record.Data(window, checkpoint, s.split))
        case Record.Data(window, checkpoint, Left(badRow)) =>
          Stream.emit(Record.Data(window, checkpoint, badRow.split))
        case Record.EndWindow(window, next, checkpoint) =>
          Stream.emit(Record.EndWindow[F, Window, (SinkPath, Transformed.Data)](window, next, checkpoint))
      }
    }
  }

  implicit class TransformedOps(t: Transformed) {
    def getPath: SinkPath = {
      val path = t match {
        case p: Transformed.Shredded =>
          val init = if (p.isGood) "output=good" else "output=bad"
          s"$init/vendor=${p.vendor}/name=${p.name}/format=${p.format.path.toLowerCase}/model=${p.model}/"
        case p: Transformed.WideRow =>
          if (p.good) "output=good/" else "output=bad/"
        case _: Transformed.Parquet => "output=good/"
      }
      SinkPath(path)
    }
    def split: (SinkPath, Transformed.Data) = (getPath, t.data)
  }
}
