/*
 * Copyright (c) 2021-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis

import java.net.URI

import org.typelevel.log4cats.slf4j.Slf4jLogger

import cats.{Applicative, Monad, Monoid, Parallel}
import cats.data.EitherT
import cats.implicits._
import cats.effect.implicits._

import cats.effect.{Clock, Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}

import fs2.{Pipe, Stream}

import blobstore.Store

import com.snowplowanalytics.snowplow.analytics.scalasdk.{Data, Event}

import com.snowplowanalytics.aws.AWSQueue

import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor}

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.{Compression, Formats}
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{ShredderValidations, Transformed}

import com.snowplowanalytics.snowplow.rdbloader.transformer.metrics.Metrics
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.parquet.ParquetSink
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.SinkPath.PathType
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks._
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.generic.{Checkpointer, Partitioned, Record}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sources.{Parsed, ParsedF}


object Processing {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  val Application: Processor = Processor(BuildInfo.name, BuildInfo.version)

  final case class SuccessfulTransformation(original: Event, output: List[Transformed])
  
  type Windowed[F[_], A] = Record[F, Window, A]
  type TransformationResult = Either[BadRow, SuccessfulTransformation]
  type TransformationResultF[F[_], C] = (List[TransformationResult], C)

  def run[F[_]: ConcurrentEffect: ContextShift: Clock: Timer: Parallel](resources: Resources[F],
                                                                        config: TransformerConfig.Stream): Stream[F, Unit] =
    config.input match {
      case TransformerConfig.StreamInput.Kinesis(appName, streamName, region, position) =>
        val source = sources.Kinesis.read[F](appName, streamName, region, position)
        runFromSource(source, resources, config)
      case TransformerConfig.StreamInput.File(dir) =>
        val source = sources.file.read[F](resources.blocker, dir)
        implicit val checkpointer = Checkpointer.noOpCheckpointer[F, Unit]
        runFromSource(source, resources, config)
    }

  def runFromSource[F[_]: ConcurrentEffect: ContextShift: Clock: Timer: Parallel, C: Checkpointer[F, *]](source: Stream[F, ParsedF[F, C]],
                                                                                                         resources: Resources[F],
                                                                                                         config: TransformerConfig.Stream): Stream[F, Unit] = {

    val transformer: Transformer[F] = config.formats match {
      case f: TransformerConfig.Formats.Shred =>
        Transformer.ShredTransformer(resources.iglu, f, resources.atomicLengths)
      case f: TransformerConfig.Formats.WideRow =>
        Transformer.WideRowTransformer(resources.iglu, f)
    }

    def windowing[A]: Pipe[F, (List[A], C), Windowed[F, List[A]]] =
      Record.windowed(Window.fromNow[F](config.windowing.toMinutes.toInt))

    val onComplete: ((Window, F[Unit], State)) => F[Unit] = {
      case (window, checkpoint, state) =>
        getOnComplete(
          resources.store,
          config.output.compression,
          transformer.typesInfo,
          config.output.path,
          resources.awsQueue,
          config.featureFlags.legacyMessageFormat,
          window,
          checkpoint,
          state
        )
    }

    source
      .through(transform(transformer, config.validations))
      .through(incrementMetrics(resources.metrics))
      .through(handleTransformResult(transformer))
      .through(windowing)
      .through(getSink[F](resources, config.output, config.formats))
      .evalMap(onComplete)
      .concurrently(resources.metrics.report)
      .concurrently(resources.telemetry.report)
  }

  /**
   * Get a callback that will be executed when window has been full written to destination
   * The callback sends an SQS message, and then checkpoints all records from the window
   */
  def getOnComplete[F[_]: Clock: ConcurrentEffect: ContextShift: Sync](
    store: Store[F],
    compression: Compression,
    getTypes: Set[Data.ShreddedType] => TypesInfo,
    outputPath: URI,
    awsQueue: AWSQueue[F],
    legacyMessageFormat: Boolean,
    window: Window,
    checkpoint: F[Unit],
    state: State
  ): F[Unit] = {

    val writeShreddingComplete = getWriteShreddingComplete[F](store, outputPath)
    Completion.seal[F](compression, getTypes, outputPath, awsQueue, legacyMessageFormat, writeShreddingComplete)(window, state) *>
    checkpoint *>
    logger[F].debug(s"ShreddingComplete message for ${window.getDir} has been sent")
  }

  /** Build a sink according to settings and pass it through `generic.Partitioned` */
  def getSink[F[_]: ConcurrentEffect: ContextShift: Timer](
    resources: Resources[F],
    config: TransformerConfig.Output,
    formats: Formats
  ): Grouping[F] = {
    
    val parquetSink = ParquetSink.parquetSink[F](resources, config.compression, config.path) _
    val nonParquetSink = Option(config.path.getScheme) match {
      case Some("file") =>
        (w: Window) => (_: State) => (k: SinkPath) =>
          file.getSink[F](resources.store, config.compression, w, k)
      case Some("s3" | "s3a" | "s3n") =>
        val (bucket, prefix) = S3.splitS3Path(S3.Folder.coerce(config.path.toString))
        (w: Window) => (_: State) => (k: SinkPath) =>
          s3.getSink[F](resources.store, bucket, prefix, config.compression, w, k)
      case _ =>
        val error = new IllegalArgumentException(s"Cannot create sink for ${config.path} Possible options are file://, s3://, s3a:// and s3n://")
        (_: Window) => (_: State) => (_: SinkPath) =>
          (_: Stream[F, Transformed.Data]) =>
            Stream.raiseError[F](error)
    }

    val parquetCombinedSink = (w: Window) => (s: State) => (k: SinkPath) => k.pathType match {
      case PathType.Good => parquetSink(w)(s)(k)
      case PathType.Bad => nonParquetSink(w)(s)(k)
    }

    val dataSink = formats match {
      case Formats.WideRow.PARQUET => parquetCombinedSink
      case _ => nonParquetSink
    }

    Partitioned.write[F, Window, SinkPath, Transformed.Data, State](dataSink)
  }

  /** Chunk-wise transforms incoming events into either a BadRow or a list of transformed outputs */
  def transform[F[_]: Concurrent: Parallel, C](transformer: Transformer[F], 
                                               validations: TransformerConfig.Validations): Pipe[F, ParsedF[F, C], TransformationResultF[F, C]] =
    _.chunks
      .flatMap { chunk =>
        chunk.last match {
          case None =>
            Stream.empty
          case Some((_, checkpoint)) =>
            Stream.eval {
              chunk
                .toList
                .map(_._1)
                .map(transformSingle(transformer, validations))
                .parSequenceN(100)
                .map(results => (results, checkpoint))
            }
        }
     }

  /** Transform a single event into either a BadRow or a list of transformed outputs */
  def transformSingle[F[_]: Monad](transformer: Transformer[F],
                                   validations: TransformerConfig.Validations)
                                   (parsed: Parsed): F[TransformationResult] = {
    val eitherT = for {
      event       <- EitherT.fromEither[F](parsed)
      _           <- EitherT.fromEither[F](ShredderValidations(Application, event, validations).toLeft(()))
      transformed <- transformer.goodTransform(event)
    } yield SuccessfulTransformation(original = event, output = transformed)

    eitherT.value
  }


  /** Unifies a stream of {Either of BadRow or Transformed outputs} into a stream of data with a path
   *  to where it should sink. Processes in batches for efficiency. */
  def handleTransformResult[F[_], C: Checkpointer[F, *]](transformer: Transformer[F]): Pipe[F, TransformationResultF[F, C], (List[(SinkPath, Transformed.Data, State)], C)] =
    _.map { case (items, checkpoint) =>
      val mapped = items.flatMap { result =>
        val state = State.fromEvent(result)
        result.map(_.output) match {
          case Left(bad) =>
            val t = transformer.badTransform(bad)
            List((t.getPath, t.data, state))
          case Right(head :: tail) =>
            (head.getPath, head.data, state) :: tail.map(t => (t.getPath, t.data, Monoid[State].empty))
          case Right(Nil) =>
            Nil
        }
      }
      (mapped, checkpoint)
    }


  def incrementMetrics[F[_]: Applicative, C](metrics: Metrics[F]): Pipe[F, TransformationResultF[F, C], TransformationResultF[F, C]] =
    _.evalTap { transformed =>
      val (good, bad) = transformed._1.partition(_.isRight)
      metrics.goodCount(good.size) *> metrics.badCount(bad.size)
    }

  def getWriteShreddingComplete[F[_]: ConcurrentEffect: ContextShift](
    store: Store[F],
    outputPath: URI
  ): (String, String) => F[Unit] = (filePath, content) =>
    Option(outputPath.getScheme) match {
      case Some("file") =>
        file.writeFile(store, outputPath, filePath, content)
      case Some("s3" | "s3a" | "s3n") =>
        val (bucket, key) = S3.splitS3Key(S3.Key.coerce(filePath.toString))
        s3.writeFile(store, bucket, key, content)
      case _ =>
        Sync[F].raiseError(new IllegalArgumentException(s"Can't determine writing function for filesystem for $outputPath"))
    }

  implicit class TransformedOps(t: Transformed) {
    def getPath: SinkPath = t match {
      case p: Transformed.Shredded =>
        val suffix = Some(s"vendor=${p.vendor}/name=${p.name}/format=${p.format.path.toLowerCase}/model=${p.model}/")
        val pathType = if (p.isGood) SinkPath.PathType.Good else SinkPath.PathType.Bad
        SinkPath(suffix, pathType)
      case p: Transformed.WideRow =>
        val suffix = None
        val pathType = if (p.good) SinkPath.PathType.Good else SinkPath.PathType.Bad
        SinkPath(suffix, pathType)
      case _: Transformed.Parquet =>
        SinkPath(None, SinkPath.PathType.Good)
    }
    def split: (SinkPath, Transformed.Data) = (getPath, t.data)
  }
}
