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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.iterator

import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data._
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed._
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.iterator.OnlyGoodDataIteratorSpec.{BadGenerator, GoodGenerator}

class GoodShredIteratorSpec extends GoodDataIteratorSpec {

  override val goodGenerator: GoodGenerator = () => Shredded.Tabular("vendor", "name", 1, DString("TEST GOOD DATA"))
  override val badGenerator: BadGenerator = () => Shredded.Json(isGood = false, "vendor", "name", 1, DString("TEST BAD DATA"))
}
