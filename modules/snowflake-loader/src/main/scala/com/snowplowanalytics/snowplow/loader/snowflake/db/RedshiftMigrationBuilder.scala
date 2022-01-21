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

import cats.{Applicative, Monad}
import cats.data.EitherT
import cats.effect.syntax.all._
import cats.implicits._
import cats.effect.{Effect, LiftIO, MonadThrow}
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap}
import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.migrations.{FlatSchema, Migration                       => DMigration, SchemaList => DSchemaList}
import com.snowplowanalytics.iglu.schemaddl.redshift.{AlterTable, AlterType, CommentOn, CreateTable => DCreateTable}
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.{DdlGenerator, MigrationGenerator}
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.{MigrationBuilder, Transaction}
import com.snowplowanalytics.snowplow.rdbloader.{LoaderAction, LoaderError, readSchemaKey}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, DiscoveryFailure, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Iglu, Logging}
import com.snowplowanalytics.snowplow.rdbloader.db.Transaction
import com.snowplowanalytics.snowplow.rdbloader.db.MigrationBuilder.Migration

class SnowflakeMigrationBuilder[F[_]: MonadThrow: Iglu: Effect: Transaction[*[_], C], C[_]: Monad: Logging: SfDao: LiftIO]
    extends MigrationBuilder[F, C] {
  override def build(discovery: DataDiscovery): F[Migration[C]] = ???
}
