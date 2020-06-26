/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.collection.convert.wrapAsScala._

import cats.data.Validated
import cats.implicits._
import cats.effect.Sync

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.{AWSSimpleSystemsManagementException, GetParameterRequest}

// This project
import com.snowplowanalytics.snowplow.rdbloader.{LoaderError, LoaderAction}
import com.snowplowanalytics.snowplow.rdbloader.utils.S3
import com.snowplowanalytics.snowplow.rdbloader.config.SnowplowConfig.SnowplowAws
import com.snowplowanalytics.snowplow.rdbloader.discovery.DiscoveryFailure.{S3Failure, DownloadFailure}


trait AWS[F[_]] {

  /** Recursively list S3 folder */
  def listS3(bucket: S3.Folder): F[Either[LoaderError, List[S3.BlobObject]]]

  /** Check if S3 key exist */
  def keyExists(key: S3.Key): F[Boolean]

  /** Download S3 key into local path */
  def downloadData(source: S3.Folder, dest: Path): LoaderAction[F, List[Path]]

  /** Upload text file */
  def putObject(key: S3.Key, data: String): LoaderAction[F, Unit]

  /** Retrieve decrypted property from EC2 Parameter Store */
  def getEc2Property(name: String): F[Array[Byte]]
}

object AWS {
  def apply[F[_]](implicit ev: AWS[F]): AWS[F] = ev

  /**
   * Create S3 client, backed by AWS Java SDK
   *
   * @param awsConfig Snowplow AWS Configuration
   * @return Snowplow-specific S3 client
   */
  def getClient[F[_]: Sync](awsConfig: SnowplowAws): F[AmazonS3] =
    Sync[F].delay(AmazonS3ClientBuilder.standard().withRegion(awsConfig.s3.region).build())

  def s3Interpreter[F[_]: Sync](client: AmazonS3): AWS[F] = new AWS[F] {

    def putObject(key: S3.Key, data: String): LoaderAction[F, Unit] = {
      val meta = new ObjectMetadata()
      meta.setContentLength(data.length)
      meta.setContentEncoding("text/plain")
      val (bucket, prefix) = S3.splitS3Key(key)
      val is = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))
      val action = Sync[F]
        .delay(client.putObject(bucket, prefix, is, meta))
        .attempt
        .map {
          case Right(_) => ().asRight
          case Left(error) =>
            val message = s"Cannot put S3 object $key, " ++ error.getMessage
            (LoaderError.LoaderLocalError(message): LoaderError).asLeft
        }
      LoaderAction(action)
    }

    private def list(str: S3.Folder): LoaderAction[F, List[S3ObjectSummary]] = {
      val (bucket, prefix) = S3.splitS3Path(str)

      val req = new ListObjectsV2Request()
        .withBucketName(bucket)
        .withPrefix(prefix)

      def keyUnfold(result: ListObjectsV2Result): Stream[S3ObjectSummary] = {
        if (result.isTruncated) {
          val loaded = result.getObjectSummaries()
          req.setContinuationToken(result.getNextContinuationToken)
          loaded.toStream #::: keyUnfold(client.listObjectsV2(req))
        } else {
          result.getObjectSummaries().toStream
        }
      }

      Sync[F].delay(keyUnfold(client.listObjectsV2(req)).filterNot(_.getSize == 0).toList)
        .attemptT
        .leftMap(e => LoaderError.DiscoveryError(List(S3Failure(e.toString))): LoaderError)
    }

    def listS3(bucket: S3.Folder): F[Either[LoaderError, List[S3.BlobObject]]] =
      list(bucket).map(summaries => summaries.map(S3.getKey)).value

    /**
     * Check if some `file` exists in S3 `path`
     *
     * @param key valid S3 key (without trailing slash)
     * @return true if file exists, false if file doesn't exist or not available
     */
    def keyExists(key: S3.Key): F[Boolean] = {
      val (bucket, s3Key) = S3.splitS3Key(key)
      val request = new GetObjectMetadataRequest(bucket, s3Key)
      Sync[F].delay(client.getObjectMetadata(request)).as(true).recover {
        case _: AmazonServiceException => false
      }
    }

    /**
     * Download contents of S3 folder into `destination`
     *
     * @param source AWS S3 folder
     * @param dest optional local path, tmp dir will be used if not specified
     * @return list of downloaded filenames
     */
    def downloadData(source: S3.Folder, dest: Path): LoaderAction[F, List[Path]] =
      list(source).flatMap { summaries =>
        val downloads = summaries.traverse { summary =>
          val bucket = summary.getBucketName
          val key = summary.getKey

          val download = for {
            s3Object <- Sync[F].delay(client.getObject(new GetObjectRequest(bucket, key)))
            destinationFile <- Sync[F].delay(Paths.get(dest.toString, key))
            result <- Sync[F].ifM(Sync[F].delay(Files.exists(destinationFile)))(
              Sync[F].pure(DownloadFailure(S3.Key.coerce(s"s3://$bucket/$key"), "File already exist").asLeft[Path]),
              for {
                _ <- Sync[F].delay(Files.createDirectories(destinationFile.getParent))
                _ <- Sync[F].delay(Files.copy(s3Object.getObjectContent, destinationFile))
              } yield destinationFile.asRight[DownloadFailure]
            )
          } yield result

          download
            .attempt
            .map {
              case Left(e) => DownloadFailure(S3.Key.coerce(s"s3://$bucket/$key"), e.toString).asLeft
              case Right(e) => e
            }

        }

        val result = downloads.map { d => d.map(_.toValidatedNel).sequence match {
          case Validated.Valid(paths) => paths.asRight
          case Validated.Invalid(failures) => (LoaderError.DiscoveryError(failures.toList): LoaderError).asLeft
        } }

        LoaderAction[F, List[Path]](result)
      }

    /**
     * Get value from AWS EC2 Parameter Store
     * @param name systems manager parameter's name with SSH key
     * @return decrypted string with key
     */
    def getEc2Property(name: String): F[Array[Byte]] = {
      val result = for {
        client <- Sync[F].delay(AWSSimpleSystemsManagementClientBuilder.defaultClient())
        req: GetParameterRequest = new GetParameterRequest().withName(name).withWithDecryption(true)
        par <- Sync[F].delay(client.getParameter(req))
      } yield par.getParameter.getValue.getBytes

      result.recoverWith {
        case e: AWSSimpleSystemsManagementException =>
          Sync[F].raiseError(LoaderError.LoaderLocalError(s"Cannot get $name EC2 property: ${e.getMessage}"))
      }
    }
  }
}

