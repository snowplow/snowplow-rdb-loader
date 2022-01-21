package com.snowplowanalytics.snowplow.loader.snowflake.db

import cats.{Monad, MonadError}
import cats.data.NonEmptyList
import cats.effect.MonadThrow
import cats.implicits._
import com.snowplowanalytics.iglu.schemaddl.redshift._
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.{Manifest, Transaction}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.rdbloader.db.Transaction
import com.snowplowanalytics.snowplow.rdbloader.db.Manifest._
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging

class RedshiftManifest[F[_]: MonadThrow: Transaction[*[_], C]: Monad: Logging, C[_]: SfDao: Monad](schema: String)
    extends Manifest[F, C] {

  implicit private val LoggerName: Logging.LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  def initialize: F[Unit] = ???

  override def add(message: LoaderMessage.ShreddingComplete): C[Unit] = ???

  override def get(base: Folder): C[Option[Entry]] = ???
}
