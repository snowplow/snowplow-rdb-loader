/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
