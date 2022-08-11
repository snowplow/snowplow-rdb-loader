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

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import org.specs2.mutable.{Specification, Tables}

class S3Spec extends Specification with Tables {
  "S3.Folder.parse()" should {
    "support s3:// prefix" >> {
      val folder = "s3://foo/"
      BlobStorage.Folder.parse(folder) must beRight
    }
    "support s3a:// prefix" >> {
      val folder = "s3a://foo/"
      BlobStorage.Folder.parse(folder) must beRight
    }
    "support s3n:// prefix" >> {
      val folder = "s3n://foo/"
      BlobStorage.Folder.parse(folder) must beRight
    }
  }

  "S3.Key.diff" should {
    "return path diff correctly" >> {
                        "parent"                 |                   "sub"                     |                "diff"             |>
      "s3://path1"                               !  "s3://path1/path2/path3/runs/run_id"       !  Some("path2/path3/runs/run_id")  |
      "s3://path1/path2"                         !  "s3://path1/path2/path3/runs/run_id"       !  Some("path3/runs/run_id")        |
      "s3://path1/path2/path3"                   !  "s3://path1/path2/path3/runs/run_id"       !  Some("runs/run_id")              |
      "s3://path1/path2/path3/runs"              !  "s3://path1/path2/path3/runs/run_id"       !  Some("run_id")                   |
      "s3://path1/path2/path3/runs/run_id"       !  "s3://path1/path2/path3/runs/run_id"       !  None                             |
      "s3://path1/path2/path3/runs/run_id/path4" !  "s3://path1/path2/path3/runs/run_id"       !  None                             |
      "s3://path1/path2/path4"                   !  "s3://path1/path2/path3/runs/run_id"       !  None                             |
      { (parent, sub, diff) =>
        BlobStorage.Folder.coerce(sub).diff(BlobStorage.Folder.coerce(parent)) must beEqualTo(diff)
      }
    }
  }
}