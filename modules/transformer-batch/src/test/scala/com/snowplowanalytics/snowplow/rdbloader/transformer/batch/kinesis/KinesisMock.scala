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

import com.amazonaws.services.kinesis.model.{PutRecordsRequest, PutRecordsResult, PutRecordsResultEntry}
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
        extractAndStoreSuccessfulRecords(request, output)

        new PutRecordsResult()
          .withRecords(mapToKinesisRecords(output): _*)
          .withFailedRecordCount(countFailedRecords(output))

      case KinesisResult.ExceptionThrown(ex) =>
        throw ex
    }
  }

  private def extractAndStoreSuccessfulRecords(request: PutRecordsRequest, outputStatus: Map[String, RecordWriteStatus]): Unit =
    request.getRecords.asScala.toList
      .flatMap { record =>
        val data = new String(record.getData.array(), UTF_8)
        outputStatus.get(data).filter(_ == Success).map(_ => data)
      }
      .foreach { data =>
        storedData += data
      }

  private def mapToKinesisRecords(output: Map[String, RecordWriteStatus]): List[PutRecordsResultEntry] =
    output.values.toList.map {
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
