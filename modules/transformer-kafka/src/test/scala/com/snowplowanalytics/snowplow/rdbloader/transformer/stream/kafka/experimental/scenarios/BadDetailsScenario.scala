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
