package com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.parquet.Field
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow

object fields {
  final case class AllFields(atomic: AtomicFields,
                             nonAtomicFields: NonAtomicFields) {
    def fieldsOnly: List[Field] = atomic.value ++ nonAtomicFields.value.map(_.field)
  }

  final case class AtomicFields(value: List[Field])
  final case class NonAtomicFields(value: List[TypedField])

  final case class TypedField(field: Field,
                              `type`: WideRow.Type,
                              lowerExclSchemaBound: Option[SchemaKey]
                             )

}
