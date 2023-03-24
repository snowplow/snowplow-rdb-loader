/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import java.util.UUID

import cats.{Applicative, Monad, Monoid, Parallel}
import cats.data.EitherT
import cats.implicits._
import cats.effect.implicits._

import cats.effect.{Async, Concurrent, Sync}

import fs2.{Pipe, Stream}
import fs2.text.utf8

import fs2.compression.{Compression => FS2Compression}

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload, Processor}
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.{Compression, Formats}
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.EventUtils.EventParser
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{ShredderValidations, Transformed}

import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.metrics.Metrics
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.parquet.ParquetSink
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks.SinkPath.PathType
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks._
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks.generic.{Partitioned, Record}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sources.{Checkpointer, Parsed, ParsedC}

object Processing {

  final case class SuccessfulTransformation(original: Event, output: List[Transformed])

  type Windowed[A, C] = Record[Window, A, C]
  type TransformationResult = Either[BadRow, SuccessfulTransformation]
  type TransformationResults[C] = (List[TransformationResult], C)
  type SerializationResults[C] = (List[(SinkPath, Transformed.Data)], State[C])

  def run[F[_]: Async: Parallel, C: Checkpointer[F, *]](
    resources: Resources[F, C],
    config: Config,
    processor: Processor
  ): Stream[F, Unit] = {
    val source = resources.inputStream.read.map(m => (parseEvent(m.content, processor, resources.eventParser), resources.checkpointer(m)))
    runFromSource(source, resources, config, processor)
  }

  def runFromSource[F[_]: Async, C: Checkpointer[F, *]](
    source: Stream[F, ParsedC[C]],
    resources: Resources[F, C],
    config: Config,
    processor: Processor
  ): Stream[F, Unit] = {
    implicit val lookup: RegistryLookup[F] = resources.registryLookup
    val transformer: Transformer[F] = config.formats match {
      case f: TransformerConfig.Formats.Shred =>
        Transformer.ShredTransformer(resources.igluResolver, resources.shredModelCache, f, processor)
      case f: TransformerConfig.Formats.WideRow =>
        Transformer.WideRowTransformer(resources.igluResolver, f, processor)
    }

    val messageProcessorVersion = Semver
      .decodeSemver(processor.version)
      .fold(e => throw new IllegalStateException(s"Cannot parse project version $e"), identity)
    val messageProcessor: LoaderMessage.Processor =
      LoaderMessage.Processor(processor.artifact, messageProcessorVersion)

    def windowing[A]: Pipe[F, (List[A], State[C]), Windowed[List[A], State[C]]] =
      Record.windowed(Window.fromNow[F](config.windowing.toMinutes.toInt))

    val onComplete: ((Window, State[C])) => F[Unit] = { case (window, state) =>
      Completion.seal[F, C](
        resources.blobStorage,
        config.output.compression,
        transformer.typesInfo,
        config.output.path,
        resources.producer,
        config.featureFlags.legacyMessageFormat,
        messageProcessor,
        window,
        state
      )
    }

    val transformedSource: Stream[F, Record[Window, List[(SinkPath, Transformed.Data)], State[C]]] =
      source
        .through(transform(transformer, config.validations, processor))
        .through(incrementMetrics(resources.metrics))
        .through(handleTransformResult(transformer))
        .through(windowing)

    val sink: Pipe[F, Record[Window, List[(SinkPath, Transformed.Data)], State[C]], Unit] =
      _.through(getSink(resources, config.output, config.formats))
        .evalMap(onComplete)

    Shutdown
      .run(transformedSource, sink)
      .concurrently(resources.metrics.report)
      .concurrently(resources.telemetry.report)
  }

  /** Build a sink according to settings and pass it through `generic.Partitioned` */
  def getSink[F[_]: Async: RegistryLookup, C: Monoid](
    resources: Resources[F, C],
    config: Config.Output,
    formats: Formats
  ): Grouping[F, C] = {

    val parquetSink = (w: Window) =>
      (s: State[C]) =>
        (k: SinkPath) =>
          ParquetSink.parquetSink[F, C](resources, config.compression, config.maxRecordsPerFile, config.path, w, s.types.toList, k)
    val nonParquetSink = (w: Window) =>
      (_: State[C]) =>
        (k: SinkPath) => {
          val blobSink =
            getBlobStorageSink(resources.blobStorage, BlobStorage.Folder.coerce(config.path.toString), config.compression, w, k)
          k.pathType match {
            case PathType.Good => blobSink
            case PathType.Bad =>
              resources.badSink match {
                case BadSink.UseBlobStorage() =>
                  blobSink
                case BadSink.UseQueue(queueProducer) =>
                  getQueueOutputSink(queueProducer)
              }
          }
        }

    val parquetCombinedSink = (w: Window) =>
      (s: State[C]) =>
        (k: SinkPath) =>
          k.pathType match {
            case PathType.Good => parquetSink(w)(s)(k)
            case PathType.Bad => nonParquetSink(w)(s)(k)
          }

    val dataSink = formats match {
      case Formats.WideRow.PARQUET => parquetCombinedSink
      case _ => nonParquetSink
    }

    Partitioned.write[F, Window, SinkPath, Transformed.Data, State[C]](dataSink, config.bufferSize)
  }

  def getBlobStorageSink[F[_]: Async: FS2Compression](
    blobStorage: BlobStorage[F],
    outputPath: BlobStorage.Folder,
    compression: Compression,
    window: Window,
    path: SinkPath
  ): Pipe[F, Transformed.Data, Unit] = {
    val (finalPipe, extension) = compression match {
      case Compression.None => (identity[Stream[F, Byte]] _, "txt")
      case Compression.Gzip => (FS2Compression[F].gzip(), "txt.gz")
    }

    in =>
      Stream.eval(Sync[F].delay(UUID.randomUUID)).flatMap { sinkId =>
        val fileOutputPath = outputPath.append(window.getDir).append(path.value).withKey(s"sink-$sinkId.$extension")
        in.mapFilter(_.str)
          .intersperse("\n")
          .through(utf8.encode[F])
          .through(finalPipe)
          .through(blobStorage.put(fileOutputPath, false))
      }
  }

  def getQueueOutputSink[F[_]](producer: Queue.ChunkProducer[F]): Pipe[F, Transformed.Data, Unit] = { in =>
    in.chunks
      .map(_.mapFilter(_.str))
      .evalMap(messages => producer.send(messages.toList))
  }

  /** Chunk-wise transforms incoming events into either a BadRow or a list of transformed outputs */
  def transform[F[_]: Concurrent: Parallel, C: Checkpointer[F, *]](
    transformer: Transformer[F],
    validations: TransformerConfig.Validations,
    processor: Processor
  ): Pipe[F, ParsedC[C], TransformationResults[C]] =
    _.chunks
      .flatMap { chunk =>
        val checkpoint = Checkpointer[F, C].combineAll(chunk.toList.map(_._2))
        Stream.eval {
          chunk.toList
            .map(_._1)
            .map(transformSingle(transformer, validations, processor))
            .parSequenceN(100)
            .map(results => (results, checkpoint))
        }
      }

  /** Transform a single event into either a BadRow or a list of transformed outputs */
  def transformSingle[F[_]: Monad](
    transformer: Transformer[F],
    validations: TransformerConfig.Validations,
    processor: Processor
  )(
    parsed: Parsed
  ): F[TransformationResult] = {
    val eitherT = for {
      event <- EitherT.fromEither[F](parsed)
      _ <- EitherT.fromEither[F](ShredderValidations(processor, event, validations).toLeft(()))
      transformed <- transformer.goodTransform(event)
    } yield SuccessfulTransformation(original = event, output = transformed)

    eitherT.value
  }

  /**
   * Unifies a stream of {Either of BadRow or Transformed outputs} into a stream of data with a path
   * to where it should sink. Processes in batches for efficiency.
   */
  def handleTransformResult[F[_], C: Checkpointer[F, *]](
    transformer: Transformer[F]
  ): Pipe[F, TransformationResults[C], SerializationResults[C]] =
    _.map { case (items, checkpointer) =>
      val state = State.fromEvents(items).withCheckpointer(checkpointer)
      val mapped = items.flatMap(
        _.fold(
          bad => transformer.badTransform(bad).split :: Nil,
          success => success.output.map(_.split)
        )
      )
      (mapped, state)
    }

  def incrementMetrics[F[_]: Applicative, C](metrics: Metrics[F]): Pipe[F, TransformationResults[C], TransformationResults[C]] =
    _.evalTap { transformed =>
      val (good, bad) = transformed._1.partition(_.isRight)
      metrics.goodCount(good.size) *> metrics.badCount(bad.size)
    }

  def parseEvent(
    record: String,
    processor: Processor,
    eventParser: EventParser
  ): Parsed =
    eventParser.parse(record).toEither.leftMap { error =>
      BadRow.LoaderParsingError(processor, error, Payload.RawPayload(record))
    }

  implicit class TransformedOps(t: Transformed) {
    def getPath: SinkPath = t match {
      case p: Transformed.Shredded =>
        val suffix = Some(
          s"vendor=${p.vendor}/name=${p.name}/format=${p.format.path.toLowerCase}/model=${p.model}/revision=${p.revision}/addition=${p.addition}"
        )
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
