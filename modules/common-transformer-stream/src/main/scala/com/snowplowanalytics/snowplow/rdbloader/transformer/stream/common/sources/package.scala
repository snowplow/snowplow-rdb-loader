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

import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

package object sources {

  /** The very initial data type that source can produce */
  type Parsed = Either[BadRow.LoaderParsingError, Event]

  /** Initial record data type and potential checkpoint action for that record */
  type ParsedC[C] = (Parsed, C)

}
