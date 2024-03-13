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

import com.snowplowanalytics.snowplow.rdbloader.common.config.Region
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.Config
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.Config.Output.BadSink.BackoffPolicy
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows.KinesisSink
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.kinesis.KinesisMock.KinesisResult
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.kinesis.KinesisMock.KinesisResult.ReceivedResponse.RecordWriteStatus.{
  Failure,
  Success
}
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.kinesis.KinesisMock.KinesisResult.{ExceptionThrown, ReceivedResponse}
import org.specs2.mutable.Specification

import scala.concurrent.duration.DurationInt

class KinesisSinkSpec extends Specification {

  val errorPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second, maxRetries = Some(2))
  val throttlingPolicy = errorPolicy.copy(maxRetries = None) // unlimited retries for throttling

  val kinesisConfig =
    Config.Output.BadSink.Kinesis("mockedStream", Region("unused"), recordLimit = 2, byteLimit = 50, errorPolicy, throttlingPolicy)

  "Kinesis sink retry policy + grouping in batches should work correctly for" >> {
    "successful result in first attempt" in {
      val inputData = List("badrow1")

      val kinesisOutput = List(
        ReceivedResponse(Map("badrow1" -> Success))
      )

      val storedData = processInput(inputData, kinesisOutput)

      storedData must containTheSameElementsAs(List("badrow1"))
    }

    "one internal error first, then success" in {
      val inputData = List("badrow1")

      val kinesisOutput = List(
        ReceivedResponse(Map("badrow1" -> Failure("Some internal error"))),
        ReceivedResponse(Map("badrow1" -> Success))
      )

      val storedData = processInput(inputData, kinesisOutput)

      storedData must containTheSameElementsAs(List("badrow1"))
    }

    "multiple internal errors, exceeding max retries limit, exception is thrown as a result" in {
      val inputData = List("badrow1")

      // max retries for non throttling errors = 2
      val kinesisOutput = List(
        ReceivedResponse(Map("badrow1" -> Failure("Some internal error"))),
        ReceivedResponse(Map("badrow1" -> Failure("Some internal error"))),
        ReceivedResponse(Map("badrow1" -> Failure("Some internal error")))
      )

      processInput(inputData, kinesisOutput) must throwA[RuntimeException]
    }

    "one throttling error first, then success" in {
      val inputData = List("badrow1")

      val kinesisOutput = List(
        ReceivedResponse(Map("badrow1" -> Failure("ProvisionedThroughputExceededException"))),
        ReceivedResponse(Map("badrow1" -> Success))
      )

      val storedData = processInput(inputData, kinesisOutput)

      storedData must containTheSameElementsAs(List("badrow1"))
    }

    "multiple throttling errors (no max retries) from kinesis first, then success" in {
      val inputData = List("badrow1")

      val kinesisOutput = List(
        ReceivedResponse(Map("badrow1" -> Failure("ProvisionedThroughputExceededException"))),
        ReceivedResponse(Map("badrow1" -> Failure("ProvisionedThroughputExceededException"))),
        ReceivedResponse(Map("badrow1" -> Failure("ProvisionedThroughputExceededException"))),
        ReceivedResponse(Map("badrow1" -> Failure("ProvisionedThroughputExceededException"))),
        ReceivedResponse(Map("badrow1" -> Failure("ProvisionedThroughputExceededException"))),
        ReceivedResponse(Map("badrow1" -> Failure("ProvisionedThroughputExceededException"))),
        ReceivedResponse(Map("badrow1" -> Success))
      )

      val storedData = processInput(inputData, kinesisOutput)

      storedData must containTheSameElementsAs(List("badrow1"))
    }

    "exception first, then success" in {
      val inputData = List("badrow1")

      val kinesisOutput = List(
        ExceptionThrown(new RuntimeException("Some internal error")),
        ReceivedResponse(Map("badrow1" -> Success))
      )

      val storedData = processInput(inputData, kinesisOutput)

      storedData must containTheSameElementsAs(List("badrow1"))
    }

    "multiple exceptions from kinesis, exceeding max retries, exception is thrown as a result" in {
      val inputData = List("badrow1")

      // max retries for non throttling errors = 2
      val kinesisOutput = List(
        ExceptionThrown(new RuntimeException("Some internal error")),
        ExceptionThrown(new RuntimeException("Some internal error")),
        ExceptionThrown(new RuntimeException("Some internal error"))
      )

      processInput(inputData, kinesisOutput) must throwA[RuntimeException]("Some internal error")

    }

    "first exception, then multiple throttling errors (no max retries) from kinesis first, then success" in {
      val inputData = List("badrow1")

      // not exceeding max retries for non throttling errors = 2
      val kinesisOutput = List(
        ExceptionThrown(new RuntimeException("Some internal error")),
        ReceivedResponse(Map("badrow1" -> Failure("Some internal error"))),
        ReceivedResponse(Map("badrow1" -> Failure("ProvisionedThroughputExceededException"))),
        ReceivedResponse(Map("badrow1" -> Failure("ProvisionedThroughputExceededException"))),
        ReceivedResponse(Map("badrow1" -> Failure("ProvisionedThroughputExceededException"))),
        ReceivedResponse(Map("badrow1" -> Success))
      )

      val storedData = processInput(inputData, kinesisOutput)

      storedData must containTheSameElementsAs(List("badrow1"))
    }

    "2 badrows are eventually stored successfully after series of retries" in {
      val inputData = List("badrow1", "badrow2")

      // not exceeding max retries for non throttling errors = 2
      val kinesisOutput = List(
        ReceivedResponse(
          Map("badrow1" -> Failure("Some internal error for badrow1"), "badrow2" -> Failure("Some internal error for badrow2"))
        ),
        ReceivedResponse(Map("badrow1" -> Success, "badrow2" -> Failure("Some internal error for badrow2"))),
        ReceivedResponse(Map("badrow2" -> Failure("ProvisionedThroughputExceededException"))),
        ReceivedResponse(Map("badrow2" -> Success))
      )

      val storedData = processInput(inputData, kinesisOutput)

      storedData must containTheSameElementsAs(List("badrow1", "badrow2"))
    }

    "request with 3 items is split into 2 batches as record limit is exceeded" in {
      val inputData = List("badrow1", "badrow2", "badrow3")

      // record limit = 2, so first response for 2 first items, second response for third item
      val kinesisOutput = List(
        ReceivedResponse(Map("badrow1" -> Success, "badrow2" -> Success)),
        ReceivedResponse(Map("badrow3" -> Success))
      )

      val storedData = processInput(inputData, kinesisOutput)

      storedData must containTheSameElementsAs(List("badrow1", "badrow2", "badrow3"))
    }

    "request with 2 items is split into 2 batches as byte limit is exceeded" in {
      val inputData = List("badrow1", "too          Long       Badrow        Content3")

      // byte limit = 50, so first response for first item, second response for second item
      val kinesisOutput = List(
        ReceivedResponse(Map("badrow1" -> Success)),
        ReceivedResponse(Map("too          Long       Badrow        Content3" -> Success))
      )

      val storedData = processInput(inputData, kinesisOutput)

      storedData must containTheSameElementsAs(List("badrow1", "too          Long       Badrow        Content3"))
    }
  }

  private def processInput(input: List[String], mockedResponses: List[KinesisResult]): List[String] = {
    val kinesis = new KinesisMock(mockedResponses.iterator)
    val writeToKinesis = kinesis.receive _
    val sink = new KinesisSink(writeToKinesis, kinesisConfig)
    sink.sink(input, partitionIndex = "1")
    kinesis.storedData.toList
  }
}
