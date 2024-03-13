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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing

import cats.effect.IO
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.FileUtils
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sources.ParsedC
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Processing
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing.TestApplication.TestProcessor
import fs2.Stream

object InputEventsProvider {

  def eventStream(inputEventsPath: String): Stream[IO, ParsedC[Unit]] =
    FileUtils
      .resourceFileStream(inputEventsPath)
      .filter(_.nonEmpty) // ignore empty lines
      .filter(!_.startsWith("//")) // ignore comment-like lines
      .map(f => (Processing.parseEvent(f, TestProcessor, Event.parser()), ()))
}
