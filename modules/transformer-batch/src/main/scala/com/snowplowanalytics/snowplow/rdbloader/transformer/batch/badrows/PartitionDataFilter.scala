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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows

import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data.DString

object PartitionDataFilter {

  type PartitionData = Iterator[Transformed]
  type GoodData = Iterator[Transformed]
  type BadData = Iterator[Transformed]

  def extractGoodAndSinkBad(
    data: PartitionData,
    partitionIndex: Int,
    badrowSink: BadrowSink
  ): GoodData = {
    val (good, bad) = splitPartitionToGoodAndBad(data)
    val badAsStrings = extractStringRepresentation(bad)

    if (badAsStrings.hasNext) {
      badrowSink.sink(badAsStrings, partitionIndex)
    }

    good
  }

  private def splitPartitionToGoodAndBad(data: PartitionData): (GoodData, BadData) =
    data.partition {
      case widerow: Transformed.WideRow => widerow.good
      case shredded: Transformed.Shredded.Json => shredded.isGood
      case _ => true
    }

  private def extractStringRepresentation(bad: BadData) =
    bad
      .map(_.data)
      .collect { case DString(value) =>
        value
      }
}
