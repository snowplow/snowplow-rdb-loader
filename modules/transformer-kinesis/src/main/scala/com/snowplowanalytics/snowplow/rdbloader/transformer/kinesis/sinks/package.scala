package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis

import fs2.Pipe
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data.ParquetData.FieldWithValue
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.generic.Record

package object sinks {
  type Grouping[F[_], C] = Pipe[F, Record[Window, List[(SinkPath, Transformed.Data)], State[C]], (Window, State[C])]

  implicit class TransformedDataOps(t: Transformed.Data) {
    def str: Option[String] = t match {
      case Transformed.Data.DString(s) => Some(s)
      case Transformed.Data.ParquetData(_) => None
    }

    def fieldValues: Option[List[FieldWithValue]] = t match {
      case Data.DString(_) => None
      case Data.ParquetData(value) => Some(value)
    }
  }
}
