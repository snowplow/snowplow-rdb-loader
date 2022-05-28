/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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

import scala.concurrent.duration.{DurationLong, FiniteDuration}

import cats.implicits._

import cats.effect.{ExitCase, Timer, Sync, ConcurrentEffect}

import fs2.{Stream, Pipe}

import com.snowplowanalytics.snowplow.rdbloader.common.S3.Key

import blobstore.s3.{S3Path, S3Store}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.{GetParameterRequest, AWSSimpleSystemsManagementException}

// This project
import com.snowplowanalytics.aws.sqs.SQS

import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.LoaderError


trait AWS[F[_]] { self =>

  /**
   * List S3 folder
   */
  def listS3(bucket: S3.Folder, recursive: Boolean): Stream[F, S3.BlobObject]

  def sinkS3(path: S3.Key, overwrite: Boolean): Pipe[F, Byte, Unit]

  def readKey(path: S3.Key): F[Either[Throwable, String]]

  /** Check if S3 key exist */
  def keyExists(key: S3.Key): F[Boolean]

  /** Retrieve decrypted property from EC2 Parameter Store */
  def getEc2Property(name: String): F[Array[Byte]]

  /** Read text payloads from SQS string */
  def readSqs(name: String, stop: Stream[F, Boolean]): Stream[F, String]
}

object AWS {
  def apply[F[_]](implicit ev: AWS[F]): AWS[F] = ev

  /**
   * Create S3 client, backed by AWS Java SDK
   *
   * @param region AWS region
   * @return Snowplow-specific S3 client
   */
  def getClient[F[_]: ConcurrentEffect](region: String): F[S3Store[F]] = {
    S3Store(S3AsyncClient.builder().region(Region.of(region)).build())
  }

  /** If we extend for exact SQS VisibilityTimeout it could be too late and SQS returns an error */
  val ExtendAllowance: FiniteDuration = 30.seconds

  def awsInterpreter[F[_]: ConcurrentEffect: Logging: Timer](client: S3Store[F], sqsVisibility: FiniteDuration): AWS[F] = new AWS[F] {
    /** * Transform S3 object summary into valid S3 key string */
    def getKey(path: S3Path): S3.BlobObject = {
      val key = S3.Key.coerce(s"s3://${path.bucket}/${path.key}")
      S3.BlobObject(key, path.meta.flatMap(_.size).getOrElse(0L))
    }

    def readKey(path: Key): F[Either[Throwable, String]] = {
      val (bucket, s3Key) = S3.splitS3Key(path)
      client
        .get(S3Path(bucket, s3Key, None), 1024)
        .compile
        .to(Array)
        .map(array => new String(array))
        .attempt
    }

    def listS3(folder: S3.Folder, recursive: Boolean): Stream[F, S3.BlobObject] = {
      val (bucket, s3Key) = S3.splitS3Path(folder)
      client.list(S3Path(bucket, s3Key, None), recursive).map(getKey)
    }

    def sinkS3(path: S3.Key, overwrite: Boolean): Pipe[F, Byte, Unit] = {
      val (bucket, s3Key) = S3.splitS3Key(path)
      client.put(S3Path(bucket, s3Key, None), overwrite)
    }

    /**
     * Check if some `key` exists in S3 `path`
     *
     * @param key valid S3 key (without trailing slash)
     * @return true if file exists, false if file doesn't exist or not available
     */
    def keyExists(key: S3.Key): F[Boolean] = {
      val (bucket, s3Key) = S3.splitS3Key(key)
      client.list(S3Path(bucket, s3Key, None)).compile.toList.map(_.nonEmpty)
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
          Sync[F].raiseError(LoaderError.RuntimeError(s"Cannot get $name EC2 property: ${e.getMessage}"))
      }
    }


    def readSqs(name: String, stop: Stream[F, Boolean]): Stream[F, String] = {
      val awakePeriod: FiniteDuration = sqsVisibility - ExtendAllowance
      SQS.readQueue(name, sqsVisibility.toSeconds.toInt, stop).flatMap { case (msg, ack, extend) =>
        Stream.emit(msg.body())
          .concurrently {
            Stream.awakeEvery[F](awakePeriod).evalMap { _ =>
              Logging[F].info(s"Approaching end of SQS message visibility. Extending visibility by $sqsVisibility.") *>
              extend(sqsVisibility)
            }
            .handleErrorWith { t =>
              Stream.eval(Logging[F].error(t)("Error extending SQS message visibility"))
            }
            .drain
          }
          .onFinalizeCase {
            case ExitCase.Canceled =>
              // The app is shutting down for a reason unrelated to processing this message.
              // E.g. handling a SIGINT, or an exception was thrown processing a _different_ message, not this one.
              ().pure[F]
            case ExitCase.Error(t) =>
              // This ExitCase means an exception was thrown upstream.
              // But for this stream that can only mean when extending the visibility -- but we already handled all errors.
              // So this case should never happen.
              Logging[F].error(t)("Unexpected error waiting for SQS message to finalize")
            case ExitCase.Completed =>
              // This ExitCase means that the message was processed downstream.
              // We get a ExitCase.Completed no matter if downstream ended in success or a raised exception.
              // We ack the message, because in either case we don't want to read the SQS message again.
              Logging[F].info(s"Acking SQS message because processing is complete.") *>
              ack
          }
      }
    }
  }
}
