/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
