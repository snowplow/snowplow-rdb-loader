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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.kinesis

import software.amazon.awssdk.services.kinesis.model.{PutRecordsRequest, PutRecordsRequestEntry, PutRecordsResponse, PutRecordsResultEntry}
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.kinesis.KinesisMock.KinesisResult
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.kinesis.KinesisMock.KinesisResult.ReceivedResponse.RecordWriteStatus
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.kinesis.KinesisMock.KinesisResult.ReceivedResponse.RecordWriteStatus.{
  Failure,
  Success
}

import java.nio.charset.StandardCharsets.UTF_8
import scala.jdk.CollectionConverters._
import scala.collection.mutable

final class KinesisMock(responses: Iterator[KinesisResult]) {
  val storedData: mutable.ListBuffer[String] = mutable.ListBuffer.empty

  def receive(request: PutRecordsRequest): PutRecordsResponse = {
    val response = responses.next()

    response match {
      case KinesisResult.ReceivedResponse(output) =>
        val requestData    = extractRequestContent(request.records().asScala.toList)
        val responseStatus = extractResponseStatus(requestData, output)

        storeSuccessfulRecords(requestData, responseStatus)

        PutRecordsResponse
          .builder()
          .records(mapToKinesisRecords(responseStatus).asJava)
          .failedRecordCount(countFailedRecords(output))
          .build()

      case KinesisResult.ExceptionThrown(ex) =>
        throw ex
    }
  }

  private def extractResponseStatus(requestData: List[String], output: Map[String, RecordWriteStatus]): List[RecordWriteStatus] =
    requestData
      .map { data =>
        output.getOrElse(data, throw new RuntimeException(s"No mapped output for value: '$data'!"))
      }

  private def storeSuccessfulRecords(requestData: List[String], responses: List[RecordWriteStatus]): Unit =
    requestData
      .zip(responses)
      .filter(_._2 == RecordWriteStatus.Success)
      .foreach { case (data, _) =>
        storedData += data
      }

  private def extractRequestContent(requestEntries: List[PutRecordsRequestEntry]): List[String] =
    requestEntries
      .map { record =>
        new String(record.data().asByteArray(), UTF_8)
      }

  private def mapToKinesisRecords(responses: List[RecordWriteStatus]): List[PutRecordsResultEntry] =
    responses.map {
      case Success =>
        PutRecordsResultEntry.builder().build()
      case Failure(errorCode) =>
        PutRecordsResultEntry.builder().errorCode(errorCode).build()
    }

  private def countFailedRecords(output: Map[String, RecordWriteStatus]): Int =
    output.values.toList.collect { case failure: RecordWriteStatus.Failure =>
      failure
    }.size
}

object KinesisMock {

  sealed trait KinesisResult

  object KinesisResult {
    final case class ExceptionThrown(ex: Throwable) extends KinesisResult
    final case class ReceivedResponse(status: Map[String, ReceivedResponse.RecordWriteStatus]) extends KinesisResult

    object ReceivedResponse {
      sealed trait RecordWriteStatus

      object RecordWriteStatus {
        case object Success extends RecordWriteStatus
        final case class Failure(errorCode: String) extends RecordWriteStatus
      }
    }
  }
}
