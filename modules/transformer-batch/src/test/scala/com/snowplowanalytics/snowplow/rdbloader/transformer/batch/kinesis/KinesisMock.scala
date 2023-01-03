/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.kinesis

import com.amazonaws.services.kinesis.model.{PutRecordsRequest, PutRecordsRequestEntry, PutRecordsResult, PutRecordsResultEntry}
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.kinesis.KinesisMock.KinesisResult
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.kinesis.KinesisMock.KinesisResult.ReceivedResponse.RecordWriteStatus
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.kinesis.KinesisMock.KinesisResult.ReceivedResponse.RecordWriteStatus.{
  Failure,
  Success
}

import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.JavaConverters._
import scala.collection.mutable

final class KinesisMock(responses: Iterator[KinesisResult]) {
  val storedData: mutable.ListBuffer[String] = mutable.ListBuffer.empty

  def receive(request: PutRecordsRequest): PutRecordsResult = {
    val response = responses.next()

    response match {
      case KinesisResult.ReceivedResponse(output) =>
        val requestData = extractRequestContent(request.getRecords.asScala.toList)
        val responseStatus = extractResponseStatus(requestData, output)

        storeSuccessfulRecords(requestData, responseStatus)

        new PutRecordsResult()
          .withRecords(mapToKinesisRecords(responseStatus): _*)
          .withFailedRecordCount(countFailedRecords(output))

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
        new String(record.getData.array(), UTF_8)
      }

  private def mapToKinesisRecords(responses: List[RecordWriteStatus]): List[PutRecordsResultEntry] =
    responses.map {
      case Success =>
        new PutRecordsResultEntry()
      case Failure(errorCode) =>
        new PutRecordsResultEntry().withErrorCode(errorCode)
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
