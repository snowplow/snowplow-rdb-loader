/*
 * Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */

package com.snowplowanalytics.snowplow.rdbloader.common

import org.specs2.mutable.Specification

class S3Spec extends Specification {
  "S3.Folder.parse()" should {
    "support s3:// prefix" >> {
      val folder = "s3://foo/"
      S3.Folder.parse(folder) must beRight
    }
    "support s3a:// prefix" >> {
      val folder = "s3a://foo/"
      S3.Folder.parse(folder) must beRight
    }
    "support s3n:// prefix" >> {
      val folder = "s3n://foo/"
      S3.Folder.parse(folder) must beRight
    }
  }
}