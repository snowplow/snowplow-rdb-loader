/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader

/**
 * Tags an exception as one for which we should not skip a batch and attempt the next batch
 *
 * Typically, if we get errors when processing a batch, then we retry it a few times and eventually
 * give up on that batch. However, there are certain classes of exception for which it is better to
 * halt processing, because skipping to the next batch will not fix anything. For example, failure
 * to connect to the warehouse.
 */
trait UnskippableException

object UnskippableException {

  /**
   * An extractor that matches if the exception, or any underlying cause of that exception, is
   * unskippable
   */
  def unapply(t: Throwable): Option[Throwable] =
    (t, Option(t.getCause)) match {
      case (_: UnskippableException, _) => Some(t)
      case (_, Some(cause)) => unapply(cause).map(_ => t)
      case (_, None) => None
    }
}
