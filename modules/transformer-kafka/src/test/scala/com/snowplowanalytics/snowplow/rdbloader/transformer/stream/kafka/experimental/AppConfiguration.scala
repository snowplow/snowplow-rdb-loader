package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.experimental
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression

final case class AppConfiguration(
  compression: Compression,
  fileFormat: WideRow.WideRowFormat,
  windowFrequencyMinutes: Long
)

object AppConfiguration {

  /**
   * Regarding `windowFrequencyMinutes = 1` - officially the default 'windowing' setting for
   * streaming transformer is '10 minutes'. As we don't want to make the tests take too much time,
   * we use 1 minute here. It means that for all test scenarios using this default confguration,
   * transformer instance under the test needs to be configured with `1 minute` windowing setting.
   *
   * Compression and file format defaults match the ones from the official reference file.
   *
   * See reference here ->
   * https://github.com/snowplow/snowplow-rdb-loader/blob/master/modules/common-transformer-stream/src/main/resources/application.conf
   */
  val default = AppConfiguration(Compression.Gzip, WideRow.WideRowFormat.JSON, windowFrequencyMinutes = 1)
}
