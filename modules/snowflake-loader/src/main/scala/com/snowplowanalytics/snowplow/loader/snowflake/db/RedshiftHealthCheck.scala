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
package com.snowplowanalytics.snowplow.loader.snowflake.db

import cats.syntax.all._
import cats.effect.{Concurrent, Timer}
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.{HealthCheck, Transaction}
import com.snowplowanalytics.snowplow.rdbloader.db.Transaction
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, Monitoring}

class RedshiftHealthCheck[F[_]: Transaction[*[_], C]: Timer: Concurrent: Logging: Monitoring, C[_]: SfDao]
    extends HealthCheck[F] {

  def perform: F[Boolean] = ???
}
