/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.dsl

import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.rdbloader.transactors.{RetryingTransactor, SSH}
import com.snowplowanalytics.snowplow.rdbloader.loading.TargetCheck
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage

class AlertSpec extends Specification {

  "Alert getMessage" should {
    "render a readable message" in {

      val e1 = new java.net.SocketException("Problem with network")
      val e2 = new SSH.SSHException(e1)
      val e3 = new RetryingTransactor.ExceededRetriesException(e2)
      val e4 = new RetryingTransactor.UnskippableConnectionException(e3)
      val e5 = new TargetCheck.TargetCheckException(e4)

      val folder = BlobStorage.Folder.coerce("s3://bucket/1/")

      val alertMessage = Alert.UnskippableLoadFailure(folder, e5)

      Alert.getMessage(
        alertMessage
      ) must_== "Load failed and will be retried until fixed: Failed to establish a JDBC connection: Error setting up SSH tunnel: Problem with network"
    }

    "not exceed the maximum length for an alert message" in {
      val longMsg = "x" * 10000

      val e = new RuntimeException(longMsg)
      val folder = BlobStorage.Folder.coerce("s3://bucket/1/")
      val alertMessage = Alert.UnskippableLoadFailure(folder, e)

      Alert.getMessage(alertMessage) must haveSize(4096)
    }
  }
}
