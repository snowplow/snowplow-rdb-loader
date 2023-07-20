/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
    override def hadoopConf: Configuration = new Configuration()
  }
}
