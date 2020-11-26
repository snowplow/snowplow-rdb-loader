package com.snowplowanalytics.snowplow.storage.spark

import com.snowplowanalytics.iglu.core.SchemaKey

final case class SqsMessage(
  shreddedTypes: List[ShreddedType]
)

final case class ShreddedType(
  schemaKey: SchemaKey,
  s3Path: String,
  format: ShreddedFormat
)

sealed trait ShreddedFormat extends Product with Serializable
object ShreddedFormat {
  case object Json extends ShreddedFormat
  case object Tsv extends ShreddedFormat
}