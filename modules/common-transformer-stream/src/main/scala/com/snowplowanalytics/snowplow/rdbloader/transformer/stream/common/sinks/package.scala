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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import fs2.Pipe
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data.ParquetData.FieldWithValue
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks.generic.Record

package object sinks {
  type Grouping[F[_], C] = Pipe[F, Record[Window, List[(SinkPath, Transformed.Data)], State[C]], (Window, State[C])]

  implicit class TransformedDataOps(t: Transformed.Data) {
    def str: Option[String] = t match {
      case Transformed.Data.DString(s)     => Some(s)
      case Transformed.Data.ParquetData(_) => None
    }

    def fieldValues: Option[List[FieldWithValue]] = t match {
      case Data.DString(_)         => None
      case Data.ParquetData(value) => Some(value)
    }
  }
}
