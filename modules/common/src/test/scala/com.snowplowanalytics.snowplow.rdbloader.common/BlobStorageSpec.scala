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

package com.snowplowanalytics.snowplow.rdbloader.common

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import org.specs2.mutable.{Specification, Tables}

class BlobStorageSpec extends Specification with Tables {
  "BlobStorage.Folder.parse()" should {
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
    "support gs:// prefix" >> {
      val folder = "gs://foo/"
      BlobStorage.Folder.parse(folder) must beRight
    }
  }

  "BlobStorage.Key.diff" should {
    "return path diff correctly" >> {
      "parent" | "sub" | "diff" |>
        "s3://path1" ! "s3://path1/path2/path3/runs/run_id" ! Some("path2/path3/runs/run_id") |
        "s3://path1/path2" ! "s3://path1/path2/path3/runs/run_id" ! Some("path3/runs/run_id") |
        "s3://path1/path2/path3" ! "s3://path1/path2/path3/runs/run_id" ! Some("runs/run_id") |
        "s3://path1/path2/path3/runs" ! "s3://path1/path2/path3/runs/run_id" ! Some("run_id") |
        "s3://path1/path2/path3/runs/run_id" ! "s3://path1/path2/path3/runs/run_id" ! None |
        "s3://path1/path2/path3/runs/run_id/path4" ! "s3://path1/path2/path3/runs/run_id" ! None |
        "s3://path1/path2/path4" ! "s3://path1/path2/path3/runs/run_id" ! None | { (parent, sub, diff) =>
          BlobStorage.Folder.coerce(sub).diff(BlobStorage.Folder.coerce(parent)) must beEqualTo(diff)
        }
    }
  }
}
