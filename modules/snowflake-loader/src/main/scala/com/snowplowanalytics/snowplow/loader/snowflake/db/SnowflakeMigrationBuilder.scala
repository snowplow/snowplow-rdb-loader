/*
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd. All rights reserved.
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

import cats.Monad
import cats.data._
import cats.implicits._

import com.snowplowanalytics.iglu.schemaddl.migrations.{SchemaList => DSchemaList}
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.MigrationBuilder
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import com.snowplowanalytics.snowplow.rdbloader._

class SnowflakeMigrationBuilder[C[_]: Monad: Logging: SfDao](dbSchema: String) extends MigrationBuilder[C] {
  override def build(schemas: List[DSchemaList]): LoaderAction[C, MigrationBuilder.Migration[C]] =
    EitherT.liftF(Logging[C].info(dbSchema) *> Monad[C].pure(MigrationBuilder.Migration(Monad[C].unit, Monad[C].unit)))
}
