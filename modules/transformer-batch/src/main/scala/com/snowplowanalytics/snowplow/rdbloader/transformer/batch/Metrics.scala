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
