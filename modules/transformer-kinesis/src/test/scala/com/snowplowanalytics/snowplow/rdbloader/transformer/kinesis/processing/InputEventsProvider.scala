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
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.TransformingSpec.testBlocker
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sources.{ParsedF, file => FileSource}
import fs2.Stream


object InputEventsProvider {

  def eventStream(inputEventsPath: String)
                 (implicit cs: ContextShift[IO]): Stream[IO, ParsedF[IO, IO[Unit]]] = {
    FileUtils.resourceFileStream(testBlocker, inputEventsPath)
      .filter(_.nonEmpty) // ignore empty lines
      .filter(!_.startsWith("//")) // ignore comment-like lines
      .map(f => (FileSource.parse(f), IO.pure(())))
  }
}
