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

import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.amazonaws.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataRequest, StandardUnit}

object Metrics {

  private lazy val cloudwatch = AmazonCloudWatchClientBuilder.defaultClient()

  def sendLatency(
    latencyMs: Long,
    namespace: String,
    metricName: String,
    dimensions: Map[String, String]
  ): Unit = {
    val datum = new MetricDatum()
      .withMetricName(metricName)
      .withUnit(StandardUnit.Milliseconds)
      .withValue(latencyMs.toDouble)
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
        System.out.println(s"$metricName with value $latencyMs ms successfully sent to namespace $namespace")
      case Failure(err) =>
        System.err.println(s"Couldn't send $metricName with value $latencyMs ms to namespace $namespace. Error: ${err.getMessage()}")
    }
  }
}
