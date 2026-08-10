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

import org.specs2.mutable.Specification

class ArchivalSpec extends Specification {

  "Archival.extractFileName" should {
    "return the last path component from a slash-delimited key" in {
      Archival.extractFileName("some/path/to/file.gz") must_== "file.gz"
    }
    "return the key unchanged when there is no slash" in {
      Archival.extractFileName("file.gz") must_== "file.gz"
    }
    "return an empty string for a key ending with a slash" in {
      Archival.extractFileName("some/path/") must_== ""
    }
  }
}
