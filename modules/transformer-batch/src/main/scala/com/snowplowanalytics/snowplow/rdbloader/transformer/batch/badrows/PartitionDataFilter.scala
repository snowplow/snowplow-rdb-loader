package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows

import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed

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

    if (bad.hasNext) {
      val badrowAsStrings = bad
        .collect { case Transformed.WideRow(false, dString) => dString.value }

      badrowSink.sink(badrowAsStrings, partitionIndex)
    }

    good
  }

  private def splitPartitionToGoodAndBad(data: PartitionData): (GoodData, BadData) =
    data.partition {
      case Transformed.WideRow(isGood, _) => isGood
      case _ => true
    }
}
