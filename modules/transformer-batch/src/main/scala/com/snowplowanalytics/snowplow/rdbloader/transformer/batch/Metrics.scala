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

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration.FiniteDuration

import software.amazon.awssdk.services.cloudwatch.CloudWatchClient
import software.amazon.awssdk.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataRequest, StandardUnit}

object Metrics {

  private lazy val cloudwatch = CloudWatchClient.builder().build()

  def sendDuration(
    namespace: String,
    metricName: String,
    duration: FiniteDuration,
    dimensions: Map[String, String]
  ): Unit = {
    val dimensionsList = dimensions
      .map { case (k, v) =>
        Dimension.builder().name(k).value(v).build()
      }
      .toSeq
      .asJava

    val datum = MetricDatum
      .builder()
      .metricName(metricName)
      .unit(StandardUnit.MILLISECONDS)
      .value(duration.toMillis.toDouble)
      .dimensions(dimensionsList)
      .build()

    val putRequest = PutMetricDataRequest
      .builder()
      .namespace(namespace)
      .metricData(datum)
      .build()

    Try(cloudwatch.putMetricData(putRequest)) match {
      case Success(_) =>
        System.out.println(s"$metricName with value $duration successfully sent to namespace $namespace")
      case Failure(err) =>
        System.err.println(s"Couldn't send $metricName with value $duration to namespace $namespace. Error: ${err.getMessage()}")
    }
  }
}
