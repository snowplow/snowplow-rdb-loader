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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.parquet

import org.apache.hadoop.conf.Configuration

trait ParquetOps {
  def transformPath(p: String): String
  def hadoopConf: Configuration
}

object ParquetOps {
  def noop: ParquetOps = new ParquetOps {
    override def transformPath(p: String): String = p
    override def hadoopConf: Configuration        = new Configuration()
  }
}
