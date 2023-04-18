/*
 * Copyright (c) 2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration.FiniteDuration

import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.amazonaws.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataRequest, StandardUnit}

object Metrics {

  private lazy val cloudwatch = AmazonCloudWatchClientBuilder.defaultClient()

  def sendDuration(
    namespace: String,
    metricName: String,
    duration: FiniteDuration,
    dimensions: Map[String, String]
  ): Unit = {
    val datum = new MetricDatum()
      .withMetricName(metricName)
      .withUnit(StandardUnit.Milliseconds)
      .withValue(duration.toMillis)
      .withDimensions(
        dimensions
          .map { case (k, v) =>
            new Dimension().withName(k).withValue(v)
          }
          .toSeq
          .asJava
      )

    val putRequest = new PutMetricDataRequest()
      .withNamespace(namespace)
      .withMetricData(datum)

    Try(cloudwatch.putMetricData(putRequest)) match {
      case Success(_) =>
        System.out.println(s"$metricName with value $duration successfully sent to namespace $namespace")
      case Failure(err) =>
        System.err.println(s"Couldn't send $metricName with value $duration to namespace $namespace. Error: ${err.getMessage()}")
    }
  }
}
