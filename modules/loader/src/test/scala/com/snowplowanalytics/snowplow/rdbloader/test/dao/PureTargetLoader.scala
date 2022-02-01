package com.snowplowanalytics.snowplow.rdbloader.test.dao

import com.snowplowanalytics.snowplow.rdbloader.algebras.db.TargetLoader
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.test.Pure

object PureTargetLoader {
  def interpreter: TargetLoader[Pure] = new TargetLoader[Pure] {
    def run(discovery: DataDiscovery): Pure[Unit] = Pure.sql(s"discovery ${discovery.base}")
  }
}
