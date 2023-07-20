/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.parquet.Field
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow

object fields {
  final case class AllFields(atomic: AtomicFields, nonAtomicFields: NonAtomicFields) {
    def fieldsOnly: List[Field] = atomic.value ++ nonAtomicFields.value.map(_.field)
  }

  final case class AtomicFields(value: List[Field])
  final case class NonAtomicFields(value: List[TypedField])

  final case class TypedField(
    field: Field,
    `type`: WideRow.Type,
    matchingKeys: Set[SchemaKey]
  )

}
