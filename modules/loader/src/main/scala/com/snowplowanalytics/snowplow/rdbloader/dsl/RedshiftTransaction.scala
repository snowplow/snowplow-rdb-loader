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

import cats.effect.{ContextShift, Blocker, Resource, Timer, ConcurrentEffect}

import doobie._

import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget

object  RedshiftTransaction {

  val RedshiftDriver = "com.amazon.redshift.jdbc42.Driver"

  def interpreter[F[_]: ConcurrentEffect: ContextShift: Timer: AWS](target: StorageTarget.Redshift, blocker: Blocker): Resource[F, Transaction[F, ConnectionIO]] = {
    val url = s"jdbc:redshift://${target.host}:${target.port}/${target.database}"
    val props = target.jdbc.properties
    Transaction.buildPool[F](target.password, url, target.username, RedshiftDriver, props, blocker)
      .map(xa => Transaction.jdbcRealInterpreter[F](xa))
  }
}
