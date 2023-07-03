package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.experimental.scenarios

import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.experimental.InputBatch.Content
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.experimental.TransformerSpecification.CountExpectations
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.experimental.{
  AppConfiguration,
  AzureTransformerSpecification,
  InputBatch
}

class BadDetailsScenario extends AzureTransformerSpecification {

  private val badEvent = Content.TextLines(List("Some example bad event"))

  override def description = "Asserting details of output single bad row"
  override def requiredAppConfig = AppConfiguration.default
  override def inputBatches = List(InputBatch(badEvent))
  override def countExpectations = CountExpectations(good = 0, bad = 1)

  override def customDataAssertion = Some { outputData =>
    val badRow = outputData.bad.head
    badRow.hcursor.get[String]("schema").right.get must beEqualTo(
      "iglu:com.snowplowanalytics.snowplow.badrows/loader_parsing_error/jsonschema/2-0-0"
    )
    badRow.hcursor.downField("data").get[String]("payload").right.get must beEqualTo("Some example bad event")
    badRow.hcursor.downField("data").downField("failure").get[String]("type").right.get must beEqualTo("NotTSV")
  }
}
