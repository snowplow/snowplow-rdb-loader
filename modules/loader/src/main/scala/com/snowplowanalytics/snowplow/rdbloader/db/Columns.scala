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
package com.snowplowanalytics.snowplow.rdbloader.db

object Columns {

  final case class ColumnName(value: String) extends AnyVal
  type EventTableColumns = List[ColumnName]

  final case class ColumnsToCopy(names: List[ColumnName]) extends AnyVal

  final case class ColumnsToSkip(names: List[ColumnName]) extends AnyVal

  object ColumnsToSkip {
    val none = ColumnsToSkip(List.empty)
  }

}
