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
package com.snowplowanalytics.snowplow.loader.snowflake

import cats.effect.{ExitCode, IO, IOApp}

import com.snowplowanalytics.snowplow.rdbloader.Runner

object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] =
    Runner.run[IO](
      args,
      Snowflake.build,
      "rdb-loader-snowflake"
    )
}
