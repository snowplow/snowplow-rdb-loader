package com.snowplowanalytics.snowplow.storage.spark

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

object BadRowSchemas {
  val ShreddingError = SchemaKey("com.snowplowanalytics.snowplow.badrows", "rdb_loader_shredding_error", "jsonschema", SchemaVer.Full(1, 0, 0))
  val ValidationError = SchemaKey("com.snowplowanalytics.snowplow.badrows", "rdb_loader_validation_error", "jsonschema", SchemaVer.Full(1, 0, 0))
  val DeduplicationError = SchemaKey("com.snowplowanalytics.snowplow.badrows", "rdb_loader_deduplication_error", "jsonschema", SchemaVer.Full(1, 0, 0))
}
