/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
