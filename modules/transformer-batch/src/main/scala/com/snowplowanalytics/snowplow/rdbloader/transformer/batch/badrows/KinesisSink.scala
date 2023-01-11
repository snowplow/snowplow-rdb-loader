package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows

import cats.implicits.{catsSyntaxEq, toFoldableOps}
import cats.{Id, Monoid}
import com.amazonaws.services.kinesis.model.{PutRecordsRequest, PutRecordsRequestEntry, PutRecordsResult}
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows.KinesisSink.{
  Batch,
  FlushDataToKinesis,
  KeyedData,
  TryBatchResult,
  policyForErrors,
  policyForThrottling,
  sleepId
}
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.{Cloud, Config}
import retry.syntax.all._
import retry.{RetryPolicies, RetryPolicy, Sleep}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.JavaConverters._
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Random

final class KinesisSink(
  flushDataToKinesis: FlushDataToKinesis,
  streamName: String,
  recordLimit: Int,
  byteLimit: Int
) extends BadrowSink {

  override def sink(badrows: Iterator[String], partitionIndex: Int): Unit = {
    var currentBatch = Batch.empty(partitionIndex)

    while (badrows.hasNext) {
      val data = KeyedData(key = Random.nextInt.toString, content = badrows.next().getBytes(UTF_8))

      currentBatch = sinkIfLimitsExceeded(currentBatch, data)
        .addData(data)
    }

    sinkPendingDataIn(currentBatch)
  }

  private def sinkIfLimitsExceeded(currentBatch: Batch, data: KeyedData): Batch =
    if (recordLimitExceeded(currentBatch) || byteLimitExceeded(currentBatch, data)) {
      writeBatch(currentBatch)
      Batch.empty(currentBatch.partitionIndex)
    } else {
      currentBatch
    }

  private def recordLimitExceeded(currentBatch: Batch): Boolean =
    currentBatch.recordsCount + 1 > recordLimit

  private def byteLimitExceeded(currentBatch: Batch, data: KeyedData): Boolean =
    currentBatch.size + data.size > byteLimit

  private def sinkPendingDataIn(currentBatch: Batch): Unit =
    if (currentBatch.keyedData.nonEmpty) {
      writeBatch(currentBatch)
    }

  private def writeBatch(batch: Batch): Unit = {
    println(s"Writing batch: ${batch.asString} of badrows to Kinesis")
    var recordsToAttempt = buildRecords(batch.keyedData)

    val result = attemptToWriteRecords(recordsToAttempt).retryingOnFailures(
      policy = policyForThrottling,
      wasSuccessful = failedEntries => failedEntries.isEmpty,
      onFailure = { case (pendingFailedEntries, retryDetails) =>
        recordsToAttempt = pendingFailedEntries
        println(
          s"Writing records to Kinesis failed for batch: ${batch.asString}. Number of records left: ${recordsToAttempt.size}. Retries so far: ${retryDetails.retriesSoFar}, giving up: ${retryDetails.givingUp}"
        )
      }
    )

    if (result.nonEmpty) {
      throw new RuntimeException(s"Writing batch: ${batch.asString} to Kinesis failed")
    } else {
      println(s"Writing batch: ${batch.asString} of badrows to Kinesis was successful")
    }
  }

  private def buildRecords(keyedData: List[KeyedData]): List[PutRecordsRequestEntry] =
    keyedData.reverse.map { data =>
      new PutRecordsRequestEntry()
        .withPartitionKey(data.key)
        .withData(ByteBuffer.wrap(data.content))
    }

  private def attemptToWriteRecords(requestEntries: List[PutRecordsRequestEntry]): Id[List[PutRecordsRequestEntry]] = {
    val result = executeKinesisRequest(requestEntries).retryingOnFailures(
      policy = policyForErrors,
      wasSuccessful = r => !r.shouldRetrySameBatch,
      onFailure = { case (_, retryDetails) =>
        println(
          s"Whole batch of records failed (no success nor throttle errors). Retries so far: ${retryDetails.retriesSoFar}, giving up: ${retryDetails.givingUp}"
        )
      }
    )

    if (result.shouldRetrySameBatch)
      throw new RuntimeException("Writing batch failed")
    else
      result.nextBatchAttempt.toList
  }

  private def executeKinesisRequest(requestEntires: List[PutRecordsRequestEntry]): Id[TryBatchResult] = {
    // Data was prepended (when building batch) to 'keyedData' list, which results in reversed original order of items.
    // We have to 'reverse' it now to bring it back.
    val putRecordsRequest =
      new PutRecordsRequest()
        .withStreamName(streamName)
        .withRecords(requestEntires.asJava)

    val response = flushDataToKinesis(putRecordsRequest)
    TryBatchResult.build(requestEntires, response)
  }
}

object KinesisSink {
  type FlushDataToKinesis = PutRecordsRequest => PutRecordsResult
  type FailedWriteEntries = List[PutRecordsRequestEntry]

  private val policyForErrors = Retries.fullJitter(BackoffPolicy(minBackoff = 100.millis, maxBackoff = 10.seconds, maxRetries = Some(10)))
  private val policyForThrottling = Retries.fibonacci(BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second, maxRetries = None))

  final case class BackoffPolicy(
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    maxRetries: Option[Int]
  )

  implicit val sleepId: Sleep[Id] = new Sleep[Id] {
    override def sleep(delay: FiniteDuration): Id[Unit] =
      Thread.sleep(delay.toMillis)
  }

  case class TryBatchResult(
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

  def createFrom(config: Config.Output.BadSink.Kinesis): KinesisSink = {
    val client = Cloud.createKinesisClient(config.region)
    val flushDataToKinesis: FlushDataToKinesis = client.putRecords

    new KinesisSink(flushDataToKinesis, config.streamName, config.recordLimit, config.byteLimit)
  }

  final case class KeyedData(key: String, content: Array[Byte]) {
    val size = content.length + key.getBytes(UTF_8).length
  }

  final case class Batch(
    partitionIndex: Int,
    size: Int,
    recordsCount: Int,
    keyedData: List[KeyedData]
  ) {
    def addData(data: KeyedData): Batch =
      Batch(this.partitionIndex, size + data.size, recordsCount + 1, data :: keyedData)

    def asString: String = s"Partition: $partitionIndex, size in bytes: $size, records count: $recordsCount"
  }

  object Batch {
    def empty(partition: Int) = Batch(partition, size = 0, recordsCount = 0, keyedData = List.empty)
  }

  object Retries {

    def fullJitter(config: BackoffPolicy): RetryPolicy[Id] =
      capBackoffAndRetries(config, RetryPolicies.fullJitter[Id](config.minBackoff))

    def fibonacci(config: BackoffPolicy): RetryPolicy[Id] =
      capBackoffAndRetries(config, RetryPolicies.fibonacciBackoff[Id](config.minBackoff))

    private def capBackoffAndRetries(config: BackoffPolicy, policy: RetryPolicy[Id]): RetryPolicy[Id] = {
      val capped = RetryPolicies.capDelay[Id](config.maxBackoff, policy)
      config.maxRetries.fold(capped)(max => capped.join(RetryPolicies.limitRetries(max)))
    }
  }
}
