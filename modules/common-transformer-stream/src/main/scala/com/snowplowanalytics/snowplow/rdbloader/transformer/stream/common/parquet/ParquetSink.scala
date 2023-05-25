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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.parquet

import cats.data.EitherT
import cats.Monad
import cats.effect.{Async, Clock}
import cats.implicits._
import com.github.mjakubowski84.parquet4s.{ParquetWriter, Path, RowParquetRecord}
import com.github.mjakubowski84.parquet4s.parquet.viaParquet
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data
import com.snowplowanalytics.snowplow.badrows.FailureDetails
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.loaderIgluErrorShow
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data.ParquetData.FieldWithValue
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields.AllFields
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.{AtomicFieldsProvider, NonAtomicFieldsProvider}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.parquet.Codecs._
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks.{SinkPath, TransformedDataOps, Window}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Resources
import fs2.{Pipe, Stream}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.MessageType
import java.net.URI

object ParquetSink {

  def parquetSink[F[_]: Async: RegistryLookup, C](
    resources: Resources[F, C],
    compression: Compression,
    maxRecordsPerFile: Long,
    uri: URI,
    window: Window,
    types: List[Data.ShreddedType],
    path: SinkPath
  ): Pipe[F, Transformed.Data, Unit] = { transformedData =>
    // As uri can use 's3a' schema, using methods from 'java.nio.file.Path' would require additional dependency responsible for adding appropriate 'java.nio.file.spi.FileSystemProvider', see e.g. https://github.com/carlspring/s3fs-nio/
    // Simple strings concat works for both cases: uri configured with and without trailing '/', bypassing usage of 'java.nio.file.Path'
    val targetPath = s"${resources.parquetOps.transformPath(uri.toString)}/${window.getDir}/${path.value}"
    val schemaCreation = createSchemaFromTypes(resources, types).value

    Stream.eval(schemaCreation).flatMap {
      case Left(error) =>
        Stream.raiseError[F](new RuntimeException(s"Error while building parquet schema. ${error.show}"))
      case Right(schema) =>
        val parquetPipe = writeAsParquet(compression, targetPath, maxRecordsPerFile, schema, resources.parquetOps.hadoopConf)

        transformedData
          .mapFilter(_.fieldValues)
          .through(parquetPipe)
          .map(_ => ())
    }
  }

  private def createSchemaFromTypes[F[_]: Monad: Clock: RegistryLookup, C](
    resources: Resources[F, C],
    types: List[Data.ShreddedType]
  ): EitherT[F, FailureDetails.LoaderIgluError, MessageType] =
    for {
      nonAtomic <- NonAtomicFieldsProvider.build[F](resources.igluResolver, types.map(WideRow.Type.from))
      allFields = AllFields(AtomicFieldsProvider.static, nonAtomic)
    } yield ParquetSchema.build(allFields)

  private def writeAsParquet[F[_]: Async](
    compression: Compression,
    path: String,
    maxRecordsPerFile: Long,
    schema: MessageType,
    hadoopConf: Configuration
  ) = {
    implicit val targetSchema = schema

    val compressionCodecName = compression match {
      case Compression.None => CompressionCodecName.UNCOMPRESSED
      case Compression.Gzip => CompressionCodecName.GZIP
    }

    viaParquet[F]
      .of[List[FieldWithValue]]
      .preWriteTransformation(buildParquetRecord)
      .maxCount(maxRecordsPerFile)
      .options(ParquetWriter.Options(compressionCodecName = compressionCodecName, hadoopConf = hadoopConf))
      .write(Path(path))
  }

  private def buildParquetRecord(fieldsWithValues: List[FieldWithValue]) = Stream.emit {
    fieldsWithValues
      .foldLeft[RowParquetRecord](RowParquetRecord()) { case (acc, fieldWithValue) =>
        acc.updated(fieldWithValue.field.name, fieldWithValue.value, config)
      }
  }
}
