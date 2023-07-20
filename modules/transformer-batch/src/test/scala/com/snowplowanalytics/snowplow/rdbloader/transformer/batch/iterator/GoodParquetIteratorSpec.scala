/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.iterator

import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data._
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed._
import OnlyGoodDataIteratorSpec.{BadGenerator, GoodGenerator}

class GoodParquetIteratorSpec extends GoodDataIteratorSpec {

  override val goodGenerator: GoodGenerator = () => Parquet(ParquetData(List(ParquetData.FieldWithValue(null, null))))
  override val badGenerator: BadGenerator = () => WideRow(good = false, DString("TEST BAD DATA"))
}
