/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import java.util.Properties

import cats.effect.{ContextShift, Blocker, Resource, Timer, ConcurrentEffect}

import doobie._

import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget

object SnowflakeTransaction {

  val SnowflakeDriver = "net.snowflake.client.jdbc.SnowflakeDriver"

  def interpreter[F[_]: ConcurrentEffect: ContextShift: Timer: AWS](config: StorageTarget.Snowflake, blocker: Blocker, appName: String = "Snowplow_OSS"): Resource[F, Transaction[F, ConnectionIO]] = {
    val host = snowflakeHost(config)
    val url = s"jdbc:snowflake://$host"
    val props: Properties = createProps(config, appName)
    Transaction.buildPool[F](config.password, url, config.username, SnowflakeDriver, props, blocker)
      .map(xa => Transaction.jdbcRealInterpreter[F](xa))
  }

  private def snowflakeHost(config: StorageTarget.Snowflake): String = {
    val jdbcHost: Option[String] = config.jdbcHost
    val snowflakeRegion: String = config.snowflakeRegion
    val account: String = config.account
    // See https://docs.snowflake.com/en/user-guide/jdbc-configure.html#connection-parameters
    val AwsUsWest2Region = "us-west-2"
    // A list of AWS region names for which the Snowflake account name doesn't have the `aws` segment
    val AwsRegionsWithoutSegment = List("us-east-1", "eu-west-1", "eu-central-1", "ap-southeast-1", "ap-southeast-2")
    // A list of AWS region names for which the Snowflake account name requires the `aws` segment
    val AwsRegionsWithSegment = List("us-east-2", "us-east-1-gov", "ca-central-1", "eu-west-2", "ap-northeast-1", "ap-south-1")
    val GcpRegions = List("us-central1", "europe-west2", "europe-west4")
    //val AzureRegions = List("west-us-2", "central-us", "east-us-2", "us-gov-virginia", "canada-central", "west-europe", "switzerland-north", "southeast-asia", "australia-east")

    /**
     * Host corresponds to Snowflake full account name which might include cloud platform and region
     * See https://docs.snowflake.com/en/user-guide/jdbc-configure.html#connection-parameters
     */
    jdbcHost match {
      case Some(overrideHost) => overrideHost
      case None =>
        if (snowflakeRegion == AwsUsWest2Region)
          s"${account}.snowflakecomputing.com"
        else if (AwsRegionsWithoutSegment.contains(snowflakeRegion))
          s"${account}.${snowflakeRegion}.snowflakecomputing.com"
        else if (AwsRegionsWithSegment.contains(snowflakeRegion))
          s"${account}.${snowflakeRegion}.aws.snowflakecomputing.com"
        else if (GcpRegions.contains(snowflakeRegion))
          s"${account}.${snowflakeRegion}.gcp.snowflakecomputing.com"
        else s"${account}.${snowflakeRegion}.azure.snowflakecomputing.com"
    }
  }

  private def createProps(config: StorageTarget.Snowflake, appName: String): Properties = {
    val props: Properties = new Properties()
    props.put("account", config.account)
    props.put("warehouse", config.warehouse)
    props.put("db", config.database)
    props.put("schema", config.schema)
    props.put("application", appName)
    props
  }
}
