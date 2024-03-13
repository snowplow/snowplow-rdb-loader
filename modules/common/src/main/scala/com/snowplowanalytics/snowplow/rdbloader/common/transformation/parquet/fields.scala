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
