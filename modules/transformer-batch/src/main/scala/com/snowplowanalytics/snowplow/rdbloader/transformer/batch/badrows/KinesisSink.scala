package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows

import com.amazonaws.services.kinesis.model.{PutRecordsRequest, PutRecordsRequestEntry}
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows.KinesisSink.{Batch, FlushDataToKinesis, KeyedData}
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.{Cloud, Config}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.JavaConverters._

final class KinesisSink(
  flushDataToKinesis: FlushDataToKinesis,
  streamName: String,
  recordLimit: Int,
  byteLimit: Int
) extends BadrowSink {

  override def sink(badrows: Iterator[String], partitionIndex: Int): Unit = {
    var currentBatch = Batch.empty

    while (badrows.hasNext) {
      val data = KeyedData(key = partitionIndex.toString, content = badrows.next().getBytes(UTF_8))

      currentBatch = flushIfLimitsExceeded(currentBatch, data)
        .addData(data)
    }

    flushPendingDataIn(currentBatch)
  }

  private def flushIfLimitsExceeded(currentBatch: Batch, data: KeyedData): Batch =
    if (recordLimitExceeded(currentBatch) || byteLimitExceeded(currentBatch, data)) {
      flushToKinesis(currentBatch.keyedData)
      Batch.empty
    } else {
      currentBatch
    }

  private def recordLimitExceeded(currentBatch: Batch) =
    currentBatch.count + 1 > recordLimit

  private def byteLimitExceeded(currentBatch: Batch, data: KeyedData) =
    currentBatch.size + data.size > byteLimit

  private def flushPendingDataIn(currentBatch: Batch): Unit =
    if (currentBatch.keyedData.nonEmpty) {
      flushToKinesis(currentBatch.keyedData)
    }

  private def flushToKinesis(keyedData: List[KeyedData]): Unit = {
    // Data was prepended (when building batch) to 'keyedData' list, which results in reversed original order of items.
    // We have to 'reverse' it now to bring it back.
    val prres = keyedData.reverse.map { data =>
      new PutRecordsRequestEntry()
        .withPartitionKey(data.key)
        .withData(ByteBuffer.wrap(data.content))
    }
    val putRecordsRequest =
      new PutRecordsRequest()
        .withStreamName(streamName)
        .withRecords(prres.asJava)

    flushDataToKinesis(putRecordsRequest)
  }

}

object KinesisSink {
  type FlushDataToKinesis = PutRecordsRequest => Unit

  def createFrom(config: Config.Output.BadSink.Kinesis): KinesisSink = {
    val client = Cloud.createKinesisClient(config.region)
    val flushDataToKinesis: FlushDataToKinesis = request => { client.putRecords(request); () }
    new KinesisSink(flushDataToKinesis, config.streamName, config.recordLimit, config.byteLimit)
  }

  final case class KeyedData(key: String, content: Array[Byte]) {
    val size = content.length + key.getBytes(UTF_8).length
  }

  final case class Batch(
    size: Int,
    count: Int,
    keyedData: List[KeyedData]
  ) {
    def addData(data: KeyedData): Batch =
      Batch(size + data.size, count + 1, data :: keyedData)
  }

  object Batch {
    val empty = Batch(size = 0, count = 0, keyedData = List.empty)
  }
}
