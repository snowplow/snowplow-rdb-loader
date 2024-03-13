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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import org.specs2.mutable.SpecificationLike
import org.specs2.specification.core.Fragments

/**
 * The content of `beforeAll` is executed before a spec and the content of `afterAll` is executed
 * once the spec is done. TODO: To remove once specs2 has been updated.
 */
trait BeforeAfterAll extends SpecificationLike {
  override def map(fragments: => Fragments) =
    step(beforeAll()) ^ fragments ^ step(afterAll())

  def beforeAll(): Unit
  def afterAll(): Unit
}
