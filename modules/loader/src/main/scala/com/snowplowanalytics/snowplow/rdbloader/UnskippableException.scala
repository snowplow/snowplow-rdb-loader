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
      case (_, Some(cause))             => unapply(cause).map(_ => t)
      case (_, None)                    => None
    }
}
