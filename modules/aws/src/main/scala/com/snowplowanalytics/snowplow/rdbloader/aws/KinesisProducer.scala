/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.aws

import cats.implicits._
import cats.{Applicative, Monoid, Parallel}
import cats.effect.kernel.Ref
import cats.effect.{Resource, Sync}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue
import com.snowplowanalytics.snowplow.rdbloader.common.config.Region
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import retry.syntax.all._
import retry.{RetryPolicies, RetryPolicy, Sleep}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.{Region => AWSRegion}
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.{PutRecordsRequest, PutRecordsRequestEntry, PutRecordsResponse}

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

object KinesisProducer {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def producer[F[_]: Parallel: Sync: Sleep](
    streamName: String,
    region: Region,
    customEndpoint: Option[URI],
    internalErrorsPolicy: BackoffPolicy,
    throttlingPolicy: BackoffPolicy,
    requestLimits: RequestLimits
  ): Resource[F, Queue.ChunkProducer[F]] =
    Resource
      .fromAutoCloseable(Sync[F].delay(buildKinesisClient(customEndpoint, region)))
      .map { client =>
        new Queue.ChunkProducer[F] {
          override def send(messages: List[String]): F[Unit] = {
            val records = messages.map(toKinesisRecord)

            writeToKinesis[F](internalErrorsPolicy, throttlingPolicy, requestLimits, client, streamName, records)
          }
        }
      }

  private def buildKinesisClient(customEndpoint: Option[URI], region: Region): KinesisClient = {
    val withRegion = KinesisClient.builder().region(AWSRegion.of(region.name))
    customEndpoint match {
      case Some(endpoint) => withRegion.endpointOverride(endpoint).build()
      case None => withRegion.build()
    }
  }

  private def writeToKinesis[F[_]: Parallel: Sync: Sleep](
    internalErrorsPolicy: BackoffPolicy,
    throttlingErrorsPolicy: BackoffPolicy,
    requestLimits: RequestLimits,
    kinesis: KinesisClient,
    streamName: String,
    records: List[PutRecordsRequestEntry]
  ): F[Unit] = {
    val policyForErrors = Retries.fullJitter[F](internalErrorsPolicy)
    val policyForThrottling = Retries.fibonacci[F](throttlingErrorsPolicy)

    def runAndCaptureFailures(ref: Ref[F, List[PutRecordsRequestEntry]]): F[List[PutRecordsRequestEntry]] =
      for {
        records <- ref.get
        failures <- group(records, requestLimits.recordLimit, requestLimits.bytesLimit, getRecordSize)
                      .parTraverse(g => tryWriteToKinesis(streamName, kinesis, g, policyForErrors))
        flattened = failures.flatten
        _ <- ref.set(flattened)
      } yield flattened

    for {
      ref <- Ref.of[F, List[PutRecordsRequestEntry]](records)
      failures <- runAndCaptureFailures(ref)
                    .retryingOnFailures(
                      policy = policyForThrottling,
                      wasSuccessful = entries => Sync[F].pure(entries.isEmpty),
                      onFailure = { case (result, retryDetails) =>
                        val msg = failureMessageForThrottling(result, streamName)
                        Logger[F].warn(s"$msg (${retryDetails.retriesSoFar} retries from cats-retry)")
                      }
                    )
      _ <- if (failures.isEmpty) Sync[F].unit
           else Sync[F].raiseError(new RuntimeException(failureMessageForThrottling(failures, streamName)))
    } yield ()
  }

  /**
   * This function takes a list of records and splits it into several lists, where each list is as
   * big as possible with respecting the record limit and the size limit.
   */
  private def group[A](
    records: List[A],
    recordLimit: Int,
    sizeLimit: Int,
    getRecordSize: A => Int
  ): List[List[A]] = {
    case class Batch(
      size: Int,
      count: Int,
      records: List[A]
    )

    records
      .foldLeft(List.empty[Batch]) { case (acc, record) =>
        val recordSize = getRecordSize(record)
        acc match {
          case head :: tail =>
            if (head.count + 1 > recordLimit || head.size + recordSize > sizeLimit)
              List(Batch(recordSize, 1, List(record))) ++ List(head) ++ tail
            else
              List(Batch(head.size + recordSize, head.count + 1, record :: head.records)) ++ tail
          case Nil =>
            List(Batch(recordSize, 1, List(record)))
        }
      }
      .map(_.records)
  }

  private def getRecordSize(record: PutRecordsRequestEntry) =
    record.data.asByteArray().length + record.partitionKey().getBytes(UTF_8).length

  /**
   * Try writing a batch, and returns a list of the failures to be retried:
   *
   * If we are not throttled by kinesis, then the list is empty. If we are throttled by kinesis, the
   * list contains throttled records and records that gave internal errors. If there is an
   * exception, or if all records give internal errors, then we retry using the policy.
   */
  private def tryWriteToKinesis[F[_]: Sync: Sleep](
    streamName: String,
    kinesis: KinesisClient,
    records: List[PutRecordsRequestEntry],
    retryPolicy: RetryPolicy[F]
  ): F[Vector[PutRecordsRequestEntry]] =
    Logger[F].debug(s"Writing ${records.size} records to ${streamName}") *>
      Sync[F]
        .blocking(putRecords(kinesis, streamName, records))
        .map(TryBatchResult.build(records, _))
        .retryingOnFailuresAndAllErrors(
          policy = retryPolicy,
          wasSuccessful = r => Sync[F].pure(!r.shouldRetrySameBatch),
          onFailure = { case (result, retryDetails) =>
            val msg = failureMessageForInternalErrors(records, streamName, result)
            Logger[F].error(s"$msg (${retryDetails.retriesSoFar} retries from cats-retry)")
          },
          onError = (exception, retryDetails) =>
            Logger[F]
              .error(exception)(
                s"Writing ${records.size} records to ${streamName} errored (${retryDetails.retriesSoFar} retries from cats-retry)"
              )
        )
        .flatMap { result =>
          if (result.shouldRetrySameBatch)
            Sync[F].raiseError(new RuntimeException(failureMessageForInternalErrors(records, streamName, result)))
          else
            result.nextBatchAttempt.pure[F]
        }

  private def toKinesisRecord(message: String): PutRecordsRequestEntry =
    PutRecordsRequestEntry
      .builder()
      .partitionKey(UUID.randomUUID().toString)
      .data(SdkBytes.fromString(message, UTF_8))
      .build()

  /**
   * The result of trying to write a batch to kinesis
   * @param nextBatchAttempt
   *   Records to re-package into another batch, either because of throttling or an internal error
   * @param hadSuccess
   *   Whether one or more records in the batch were written successfully
   * @param wasThrottled
   *   Whether at least one of retries is because of throttling
   * @param exampleInternalError
   *   A message to help with logging
   */
  private case class TryBatchResult(
    nextBatchAttempt: Vector[PutRecordsRequestEntry],
    hadSuccess: Boolean,
    wasThrottled: Boolean,
    exampleInternalError: Option[String]
  ) {
    // Only retry the exact same again if no record was successfully inserted, and all the errors
    // were not throughput exceeded exceptions
    def shouldRetrySameBatch: Boolean =
      !hadSuccess && !wasThrottled
  }

  private object TryBatchResult {

    implicit private def tryBatchResultMonoid: Monoid[TryBatchResult] =
      new Monoid[TryBatchResult] {
        override val empty: TryBatchResult = TryBatchResult(Vector.empty, false, false, None)
        override def combine(x: TryBatchResult, y: TryBatchResult): TryBatchResult =
          TryBatchResult(
            x.nextBatchAttempt ++ y.nextBatchAttempt,
            x.hadSuccess || y.hadSuccess,
            x.wasThrottled || y.wasThrottled,
            x.exampleInternalError.orElse(y.exampleInternalError)
          )
      }

    def build(records: List[PutRecordsRequestEntry], prr: PutRecordsResponse): TryBatchResult =
      if (prr.failedRecordCount().toInt =!= 0)
        records
          .zip(prr.records().asScala)
          .foldMap { case (orig, recordResult) =>
            Option(recordResult.errorCode()) match {
              case None =>
                TryBatchResult(Vector.empty, true, false, None)
              case Some("ProvisionedThroughputExceededException") =>
                TryBatchResult(Vector(orig), false, true, None)
              case Some(_) =>
                TryBatchResult(Vector(orig), false, false, Option(recordResult.errorMessage()))
            }
          }
      else
        TryBatchResult(Vector.empty, true, false, None)
  }

  private def putRecords(
    kinesis: KinesisClient,
    streamName: String,
    records: List[PutRecordsRequestEntry]
  ): PutRecordsResponse = {
    val putRecordsRequest = {
      val prr = PutRecordsRequest.builder()
      prr
        .streamName(streamName)
        .records(records.asJava)
      prr.build()
    }
    kinesis.putRecords(putRecordsRequest)
  }

  private def failureMessageForInternalErrors(
    records: List[PutRecordsRequestEntry],
    streamName: String,
    result: TryBatchResult
  ): String = {
    val exampleMessage = result.exampleInternalError.getOrElse("none")
    s"Writing ${records.size} records to $streamName errored with internal failures. Example error message [$exampleMessage]"
  }

  private def failureMessageForThrottling(
    records: List[PutRecordsRequestEntry],
    streamName: String
  ): String =
    s"Exceeded Kinesis provisioned throughput: ${records.size} records failed writing to $streamName."

  final case class BackoffPolicy(
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    maxRetries: Option[Int]
  )

  final case class RequestLimits(recordLimit: Int, bytesLimit: Int)

  object Retries {

    def fullJitter[F[_]: Applicative](config: BackoffPolicy): RetryPolicy[F] =
      capBackoffAndRetries(config, RetryPolicies.fullJitter[F](config.minBackoff))

    def fibonacci[F[_]: Applicative](config: BackoffPolicy): RetryPolicy[F] =
      capBackoffAndRetries(config, RetryPolicies.fibonacciBackoff[F](config.minBackoff))

    private def capBackoffAndRetries[F[_]: Applicative](config: BackoffPolicy, policy: RetryPolicy[F]): RetryPolicy[F] = {
      val capped = RetryPolicies.capDelay[F](config.maxBackoff, policy)
      config.maxRetries.fold(capped)(max => capped.join(RetryPolicies.limitRetries(max)))
    }
  }
}
