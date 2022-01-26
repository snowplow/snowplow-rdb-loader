/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.algerbas.db

import cats.{Applicative, Monad, ~>}
import cats.data.EitherT
import cats.implicits.none
import cats.syntax.all._
import com.snowplowanalytics.snowplow.rdbloader.{LoaderAction, LoaderError}
import com.snowplowanalytics.iglu.schemaddl.migrations.{SchemaList => DSchemaList}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.Iglu

trait MigrationBuilder[C[_]] {
  def build(items: List[MigrationBuilder.MigrationItem]): LoaderAction[C, MigrationBuilder.Migration[C]]
}

object MigrationBuilder {
  def apply[C[_]](implicit ev: MigrationBuilder[C]): MigrationBuilder[C] = ev

  def run[F[_]: Iglu: Transaction[*[_], C]: Monad, C[_]: MigrationBuilder: Monad](
    discovery: DataDiscovery
  ): LoaderAction[F, MigrationBuilder.Migration[C]] = {
    val schemas = discovery.shreddedTypes.filterNot(_.isAtomic).traverseFilter {
      case s @ ShreddedType.Tabular(ShreddedType.Info(_, vendor, name, model, _, _)) =>
        EitherT(Iglu[F].getSchemas(vendor, name, model)).map(l => MigrationItem(s, l).some)
      case ShreddedType.Json(_, _) =>
        EitherT.rightT[F, LoaderError](none[MigrationItem])
    }

    val s = for {
      schemasC  <- schemas.mapK(Transaction[F, C].arrowBack)
      migration <- MigrationBuilder[C].build(schemasC)
    } yield migration

    def runK: C ~> F = λ[C ~> F](s => Transaction[F, C].run(s))

    s.mapK(runK)
  }

  final case class MigrationItem(shreddedType: ShreddedType, schemaList: DSchemaList)

  final case class Migration[C[_]](preTransaction: C[Unit], inTransaction: C[Unit]) {
    def addPreTransaction(statement: C[Unit])(implicit F: Monad[C]): Migration[C] =
      Migration[C](preTransaction *> statement, inTransaction)
    def addInTransaction(statement: C[Unit])(implicit F: Monad[C]): Migration[C] =
      Migration[C](preTransaction, inTransaction *> statement)
  }
  object Migration {
    def empty[C[_]: Applicative]: Migration[C] = MigrationBuilder.Migration[C](Applicative[C].unit, Applicative[C].unit)
  }
}
