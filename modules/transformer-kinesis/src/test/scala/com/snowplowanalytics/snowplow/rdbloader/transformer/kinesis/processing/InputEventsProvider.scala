/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.processing

import cats.effect.{ContextShift, IO}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.FileUtils
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.Processing.Windowed
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.TransformingSpec.testBlocker
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.Window
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.generic.Record
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sources.{Parsed, file => FileSource}
import fs2.Stream


object InputEventsProvider {

  def eventStream(inputEventsPath: String,
                  currentWindow: Window,
                  nextWindow: Window)
                 (implicit cs: ContextShift[IO]): Stream[IO, Windowed[IO, Parsed]] = {
    val dataRecords = FileUtils.resourceFileStream(testBlocker, inputEventsPath)
      .map(FileSource.parse)
      .map(Record.Data[IO, Window, Parsed](currentWindow, Option.empty, _))

    val endingWindow = Stream.emit(Record.EndWindow[IO, Window, Parsed](currentWindow, nextWindow, IO.pure(())))
    dataRecords ++ endingWindow
  }
}
