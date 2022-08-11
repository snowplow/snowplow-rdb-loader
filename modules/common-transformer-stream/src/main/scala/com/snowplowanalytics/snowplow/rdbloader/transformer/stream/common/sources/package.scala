package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

package object sources {
  /** The very initial data type that source can produce */
  type Parsed = Either[BadRow.LoaderParsingError, Event]

  /** Initial record data type and potential checkpoint action for that record */
  type ParsedC[C] = (Parsed, C)

}
