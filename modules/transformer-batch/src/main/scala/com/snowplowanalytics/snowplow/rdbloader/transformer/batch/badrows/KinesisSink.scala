/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows

import cats.Monoid
import cats.implicits.{catsSyntaxEq, toFoldableOps}
import com.amazonaws.services.kinesis.model.{PutRecordsRequest, PutRecordsRequestEntry, PutRecordsResult}
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.Config.Output.BadSink.BackoffPolicy
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows.KinesisSink.{
  Batch,
  FailedWriteRecords,
  KeyedData,
  Retries,
  TryBatchResult,
  WriteDataToKinesis,
  sleepTryInstance
}
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.{Cloud, Config}
import retry.syntax.all._
import retry.{RetryPolicies, RetryPolicy, Sleep}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Random, Success, Try}

final class KinesisSink(
  writeDataToKinesis: WriteDataToKinesis,
  config: Config.Output.BadSink.Kinesis
) extends BadrowSink {

  override def sink(badrows: List[String], partitionIndex: String): Unit =
    groupIntoBatches(badrows, partitionIndex)
      .foreach(writeBatch)

  private def groupIntoBatches(badrows: List[String], partitionIndex: String): List[Batch] =
    badrows
      .map(KeyedData.create)
      .foldLeft(List.empty[Batch]) { case (batches, data) =>
        addDataToBatch(partitionIndex, batches, data)
      }
      .reverse

  private def addDataToBatch(
    partitionIndex: String,
    batches: List[Batch],
    data: KeyedData
  ): List[Batch] =
    batches match {
      case currentBatch :: fullBatches if shouldStartNewBatch(data, currentBatch) =>
        Batch.init(partitionIndex, data) :: currentBatch :: fullBatches
      case currentBatch :: fullBatches =>
        currentBatch.addData(data) :: fullBatches
      case Nil =>
        Batch.init(partitionIndex, data) :: Nil
    }

  private def shouldStartNewBatch(record: KeyedData, current: Batch): Boolean =
    recordLimitExceeded(current) || byteLimitExceeded(current, record)

  private def recordLimitExceeded(currentBatch: Batch): Boolean =
    currentBatch.recordsCount + 1 > config.recordLimit

  private def byteLimitExceeded(currentBatch: Batch, data: KeyedData): Boolean =
    currentBatch.size + data.size > config.byteLimit

  /**
   * Similar approach to retrying kinesis requests as in Enrich. See sink implementation -
   * https://github.com/snowplow/enrich/blob/master/modules/kinesis/src/main/scala/com/snowplowanalytics/snowplow/enrich/kinesis/Sink.scala
   * and issue explaining the usage of different policies -
   * https://github.com/snowplow/enrich/issues/697
   *
   * This method focues on retrying throttling error. Called here 'attemptToWriteBatch' method
   * focues on retrying non-throttling internal errors.
   */
  private def writeBatch(batch: Batch): Unit = {
    var recordsToWriteInBatch = buildRecords(batch.keyedData)

    println(s"Writing badrows batch: (${batch.asString}) to '${config.streamName}'")

    val result = attemptToWriteBatch(recordsToWriteInBatch)
      .retryingOnFailures(
        policy        = Retries.fibonacci(config.throttledBackoffPolicy),
        wasSuccessful = failedRecords => Try(failedRecords.isEmpty),
        onFailure = (failedRecords, retryDetails) =>
          Try {
            // Updating the list of unwritten to kinesis records so they are picked in the next retry
            recordsToWriteInBatch = failedRecords
            println(s"${failureMessageForThrottling(failedRecords)}. Retries so far: ${retryDetails.retriesSoFar}")
          }
      )

    result match {
      case Success(failedRecords) if failedRecords.isEmpty =>
        println(s"Writing badrows batch: (${batch.asString}) to '${config.streamName}' was successful")
      case Success(failedRecords) if failedRecords.nonEmpty =>
        throw new RuntimeException(failureMessageForThrottling(failedRecords))
      case Failure(exception) =>
        throw exception
    }
  }

  /**
   * Try writing a batch, and returns a list of the failures to be retried:
   *
   * If we are not throttled by kinesis, then the list is empty. If we are throttled by kinesis, the
   * list contains throttled records and records that gave internal errors. If there is an
   * exception, or if all records give internal errors, then we retry using the policy.
   */
  private def attemptToWriteBatch(records: List[PutRecordsRequestEntry]): Try[FailedWriteRecords] =
    executeKinesisRequest(records)
      .retryingOnFailuresAndAllErrors(
        policy        = Retries.fullJitter(config.backoffPolicy),
        wasSuccessful = r => Try(!r.shouldRetrySameBatch),
        onFailure = (result, retryDetails) =>
          Try(println(s"${failureMessageForInternalFailures(records, result)}. Retries so far: ${retryDetails.retriesSoFar}")),
        onError = (exception, retryDetails) =>
          Try(
            println(
              s"Writing ${records.size} records to '${config.streamName}' errored with exception: '${exception.getMessage}'. Retries so far: ${retryDetails.retriesSoFar}"
            )
          )
      )
      .flatMap { result =>
        // Reaching maximum number of retries and batch still should be retries cause there are some internal errros - there is nothing we can do hence throw an exception
        if (result.shouldRetrySameBatch) {
          Failure(new RuntimeException(failureMessageForInternalFailures(records, result)))
        } else {
          Success(result.nextBatchAttempt.toList)
        }
      }

  private def executeKinesisRequest(requestEntries: List[PutRecordsRequestEntry]): Try[TryBatchResult] = {
    val putRecordsRequest =
      new PutRecordsRequest()
        .withStreamName(config.streamName)
        .withRecords(requestEntries.asJava)

    Try(writeDataToKinesis(putRecordsRequest))
      .map(TryBatchResult.build(requestEntries, _))
  }

  private def buildRecords(keyedData: List[KeyedData]): List[PutRecordsRequestEntry] =
    keyedData.map { data =>
      new PutRecordsRequestEntry()
        .withPartitionKey(data.key)
        .withData(ByteBuffer.wrap(data.content))
    }

  private def failureMessageForInternalFailures(records: List[PutRecordsRequestEntry], result: TryBatchResult) = {
    val exampleMessage = result.exampleInternalError.getOrElse("none")
    s"Writing ${records.size} records to '${config.streamName}' errored with internal failures. Example error message [$exampleMessage]."
  }

  private def failureMessageForThrottling(records: List[PutRecordsRequestEntry]): String =
    s"Exceeded Kinesis provisioned throughput: ${records.size} records failed writing to '${config.streamName}'."
}

object KinesisSink {
  type WriteDataToKinesis = PutRecordsRequest => PutRecordsResult
  type FailedWriteRecords = List[PutRecordsRequestEntry]

  def createFrom(config: Config.Output.BadSink.Kinesis): KinesisSink = {
    val client                                 = Cloud.createKinesisClient(config.region)
    val writeDataToKinesis: WriteDataToKinesis = client.putRecords
    new KinesisSink(writeDataToKinesis, config)
  }

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
  final case class TryBatchResult(
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

  object TryBatchResult {

    implicit def tryBatchResultMonoid: Monoid[TryBatchResult] =
      new Monoid[TryBatchResult] {
        override val empty: TryBatchResult = TryBatchResult(Vector.empty, hadSuccess = false, wasThrottled = false, None)
        override def combine(x: TryBatchResult, y: TryBatchResult): TryBatchResult =
          TryBatchResult(
            x.nextBatchAttempt ++ y.nextBatchAttempt,
            x.hadSuccess || y.hadSuccess,
            x.wasThrottled || y.wasThrottled,
            x.exampleInternalError.orElse(y.exampleInternalError)
          )
      }

    def build(records: List[PutRecordsRequestEntry], result: PutRecordsResult): TryBatchResult =
      if (result.getFailedRecordCount.toInt =!= 0)
        records
          .zip(result.getRecords.asScala)
          .foldMap { case (orig, recordResult) =>
            Option(recordResult.getErrorCode) match {
              case None =>
                TryBatchResult(Vector.empty, hadSuccess = true, wasThrottled = false, None)
              case Some("ProvisionedThroughputExceededException") =>
                TryBatchResult(Vector(orig), hadSuccess = false, wasThrottled = true, None)
              case Some(_) =>
                TryBatchResult(Vector(orig), hadSuccess = false, wasThrottled = false, Option(recordResult.getErrorMessage))
            }
          }
      else
        TryBatchResult(Vector.empty, hadSuccess = true, wasThrottled = false, None)
  }

  final case class KeyedData(key: String, content: Array[Byte]) {
    val size = content.length + key.getBytes(UTF_8).length
  }

  object KeyedData {
    def create(data: String): KeyedData =
      KeyedData(key = Random.nextInt.toString, content = data.getBytes(UTF_8))
  }

  final case class Batch(
    partitionIndex: String,
    size: Int,
    recordsCount: Int,
    keyedData: List[KeyedData]
  ) {
    def addData(data: KeyedData): Batch =
      Batch(this.partitionIndex, size + data.size, recordsCount + 1, data :: keyedData)

    def asString: String = s"Partition: $partitionIndex, initial size in bytes: $size, records count: $recordsCount"
  }

  object Batch {
    def init(partitionIndex: String, data: KeyedData) = Batch(partitionIndex, size = data.size, recordsCount = 1, keyedData = List(data))
  }

  object Retries {

    def fullJitter(config: BackoffPolicy): RetryPolicy[Try] =
      capBackoffAndRetries(config, RetryPolicies.fullJitter[Try](config.minBackoff))

    def fibonacci(config: BackoffPolicy): RetryPolicy[Try] =
      capBackoffAndRetries(config, RetryPolicies.fibonacciBackoff[Try](config.minBackoff))

    private def capBackoffAndRetries(config: BackoffPolicy, policy: RetryPolicy[Try]): RetryPolicy[Try] = {
      val capped = RetryPolicies.capDelay[Try](config.maxBackoff, policy)
      config.maxRetries.fold(capped)(max => capped.join(RetryPolicies.limitRetries(max)))
    }
  }

  implicit val sleepTryInstance: Sleep[Try] = new Sleep[Try] {
    override def sleep(delay: FiniteDuration): Try[Unit] = Try {
      Thread.sleep(delay.toMillis)
    }
  }
}
