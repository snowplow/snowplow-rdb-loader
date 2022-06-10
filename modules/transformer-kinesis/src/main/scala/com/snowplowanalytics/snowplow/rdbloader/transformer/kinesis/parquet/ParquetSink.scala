/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.parquet

import cats.Functor
import cats.data.EitherT
import cats.effect.{Blocker, Concurrent, ContextShift, Timer}
import cats.implicits._
import com.github.mjakubowski84.parquet4s.parquet.viaParquet
import com.github.mjakubowski84.parquet4s.{ParquetWriter, RowParquetRecord}
import com.snowplowanalytics.snowplow.badrows.FailureDetails
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.loaderIgluErrorShow
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data.ParquetData.FieldWithValue
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields.AllFields
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.{AtomicFieldsProvider, NonAtomicFieldsProvider}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.parquet.Codecs._
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.{SinkPath, TransformedDataOps, Window}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.{Resources, State}
import fs2.{Pipe, Stream}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.MessageType

import java.net.URI

object ParquetSink {

  def parquetSink[F[_] : Concurrent : ContextShift : Timer](resources: Resources[F],
                                                            compression: Compression,
                                                            uri: URI)
                                                           (window: Window)
                                                           (path: SinkPath): Pipe[F, Transformed.Data, Unit] = {
    transformedData =>

      // As uri can use 's3a' schema, using methods from 'java.nio.file.Path' would require additional dependency responsible for adding appropriate 'java.nio.file.spi.FileSystemProvider', see e.g. https://github.com/carlspring/s3fs-nio/
      // Simple strings concat works for both cases: uri configured with and without trailing '/', bypassing usage of 'java.nio.file.Path'
      val targetPath = s"${uri.toString}/${window.getDir}/${path.value}"
      val schemaCreation = createSchemaFromTypes(resources, window).value

      Stream.eval(schemaCreation).flatMap {
        case Left(error) =>
          Stream.raiseError[F](new RuntimeException(s"Error while building parquet schema. ${error.show}"))
        case Right(schema) =>
          val parquetPipe = writeAsParquet(resources.blocker, compression, targetPath, schema)

          transformedData
            .mapFilter(_.fieldValues)
            .through(parquetPipe)
            .map(_ => ())
      }
  }


  private def createSchemaFromTypes[F[_] : Concurrent : ContextShift : Timer](resources: Resources[F],
                                                                              window: Window): EitherT[F, FailureDetails.LoaderIgluError, MessageType] = {
    for {
      typesForWindow <- EitherT.liftF(getAllWideRowTypesForWindow(resources.windows, window))
      nonAtomic <- NonAtomicFieldsProvider.build[F](resources.iglu.resolver, typesForWindow)
      allFields = AllFields(AtomicFieldsProvider.static, nonAtomic)
    } yield ParquetSchema.build(allFields)
  }

  private def getAllWideRowTypesForWindow[F[_]: Functor](state: State.Windows[F],
                                                         currentWindow: Window): F[List[WideRow.Type]] = {
    State
      .findTypesForWindow(state, currentWindow)
      .map(
        _.map(WideRow.Type.from)
          .toList
      )
  }

  private def writeAsParquet[F[_] : Concurrent : ContextShift : Timer](blocker: Blocker,
                                                                       compression: Compression,
                                                                       path: String,
                                                                       schema: MessageType) = {
    implicit val targetSchema = schema

    val compressionCodecName = compression match {
      case Compression.None => CompressionCodecName.UNCOMPRESSED 
      case Compression.Gzip => CompressionCodecName.GZIP
    }

    viaParquet[F, List[FieldWithValue]]
      .preWriteTransformation(buildParquetRecord)
      .options(ParquetWriter.Options(compressionCodecName = compressionCodecName))
      .write(blocker, path)
  }

  private def buildParquetRecord(fieldsWithValues: List[FieldWithValue]) = Stream.emit {
    fieldsWithValues
      .foldLeft[RowParquetRecord](RowParquetRecord.empty) {
        case (acc, fieldWithValue) =>
          acc.add(fieldWithValue.field.name, fieldWithValue.value, config)
      }
  }
}
