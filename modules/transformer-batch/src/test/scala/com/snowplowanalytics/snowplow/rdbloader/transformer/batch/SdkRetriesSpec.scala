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

import scala.util.Try

import org.specs2.mutable.Specification

import retry.{RetryPolicies, RetryPolicy}
import software.amazon.awssdk.core.exception.SdkClientException

class SdkRetriesSpec extends Specification {

  /** Same shape as SdkRetries.DefaultPolicy, minus the backoff, so the suite stays fast */
  val immediate: RetryPolicy[Try] = RetryPolicies.limitRetries[Try](3)

  "SdkRetries.retrying" should {
    "return the result of an action that succeeds first time, without re-running it" in {
      var attempts = 0
      val result = SdkRetries.retrying("do the thing", immediate) {
        attempts += 1
        "done"
      }

      result must_== "done"
      attempts must_== 1
    }

    "re-run the action until it succeeds" in {
      var attempts = 0
      val result = SdkRetries.retrying("do the thing", immediate) {
        attempts += 1
        if (attempts < 3) throw credentialsFailure else "done"
      }

      result must_== "done"
      attempts must_== 3
    }

    "rethrow the error once the retries are exhausted, having used the whole budget" in {
      var attempts = 0
      def action: Any = SdkRetries.retrying("do the thing", immediate) {
        attempts += 1
        throw credentialsFailure
      }

      action must throwAn[SdkClientException]
      attempts must_== 4 // the initial attempt plus three retries
    }

    "not retry a fatal error" in {
      var attempts = 0
      def action: Any = SdkRetries.retrying("do the thing", immediate) {
        attempts += 1
        throw new OutOfMemoryError("heap")
      }

      action must throwAn[OutOfMemoryError]
      attempts must_== 1
    }
  }

  /** What the SDK's credentials provider chain throws when IMDS is unreachable */
  def credentialsFailure: SdkClientException =
    SdkClientException
      .builder()
      .message(
        "Unable to load credentials from any of the providers in the chain : " +
          "[InstanceProfileCredentialsProvider(): Failed to load credentials from IMDS.]"
      )
      .build()
}
