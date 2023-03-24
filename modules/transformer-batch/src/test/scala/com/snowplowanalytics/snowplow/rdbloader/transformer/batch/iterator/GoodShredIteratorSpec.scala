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
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.iterator.OnlyGoodDataIteratorSpec.{BadGenerator, GoodGenerator}

class GoodShredIteratorSpec extends GoodDataIteratorSpec {

  override val goodGenerator: GoodGenerator = () => Shredded.Tabular("vendor", "name", 1, 0, 0, DString("TEST GOOD DATA"))
  override val badGenerator: BadGenerator = () => Shredded.Json(isGood = false, "vendor", "name", 1, 0, 0, DString("TEST BAD DATA"))
}
