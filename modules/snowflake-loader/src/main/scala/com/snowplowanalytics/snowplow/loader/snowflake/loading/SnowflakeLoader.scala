/*
 * Copyright (c) 2022-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.loader.snowflake.loading

import cats.implicits._

import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.loader.snowflake.ast.AtomicDef


object SnowflakeLoader {


  def getColumns(discovery: DataDiscovery): List[String] = {
    val atomicColumns = AtomicDef.columns.map(_.name)
    val shredTypeColumns = discovery.shreddedTypes
      .filterNot(_.isAtomic)
      .map(getShredTypeColumn)
    atomicColumns ::: shredTypeColumns
  }

  def getShredTypeColumn(shreddedType: ShreddedType): String = {
    val shredProperty = shreddedType.getSnowplowEntity.toSdkProperty
    val info = shreddedType.info
    SnowplowEvent.transformSchema(shredProperty, info.vendor, info.name, info.model)
  }

  def shreddedTypeCheck(shreddedTypes: List[ShreddedType]): Either[String, Unit] = {
    val unsupportedType = shreddedTypes.exists {
      case _: ShreddedType.Tabular | _: ShreddedType.Json => true
      case _: ShreddedType.Widerow => false
    }
    if (unsupportedType) "Snowflake Loader supports types with widerow format only. Received discovery contains types with unsupported format.".asLeft
    else ().asRight
  }
}
