/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
