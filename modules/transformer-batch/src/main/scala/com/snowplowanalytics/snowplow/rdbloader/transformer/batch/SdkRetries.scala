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

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Try

import org.slf4j.LoggerFactory

import retry.syntax.all._
import retry.{RetryDetails, RetryPolicies, RetryPolicy, Sleep}

/**
 * Retries an AWS SDK call issued from the Spark driver.
 *
 * The clients built in [[Cloud]] already retry retryable service errors, but the SDK cannot recover
 * from a failure to resolve credentials: the identity is resolved once per API call, before the
 * retry loop is entered, so every attempt within a call joins the same failed future. A transient
 * IMDS outage therefore fails the whole call. Executor-side calls survive that because Spark
 * retries the task, which starts a fresh API call; driver-side calls have nothing above them, so
 * they need an explicit retry.
 */
private[batch] object SdkRetries {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  private val MinBackoff: FiniteDuration = 1.second
  private val MaxBackoff: FiniteDuration = 30.seconds
  private val MaxRetries: Int            = 5

  val DefaultPolicy: RetryPolicy[Try] =
    RetryPolicies
      .capDelay[Try](MaxBackoff, RetryPolicies.fullJitter[Try](MinBackoff))
      .join(RetryPolicies.limitRetries(MaxRetries))

  private implicit val sleepTryInstance: Sleep[Try] = new Sleep[Try] {
    override def sleep(delay: FiniteDuration): Try[Unit] = Try(Thread.sleep(delay.toMillis))
  }

  /**
   * @param description
   *   what is being attempted, phrased to follow "Could not " in the log message
   */
  def retrying[A](description: String, policy: RetryPolicy[Try] = DefaultPolicy)(f: => A): A =
    Try(f)
      .retryingOnAllErrors(
        policy = policy,
        onError = (error: Throwable, details: RetryDetails) =>
          Try(logger.warn(s"Could not $description. Retries so far: ${details.retriesSoFar}. Error: ${error.getMessage}"))
      )
      .get
}
