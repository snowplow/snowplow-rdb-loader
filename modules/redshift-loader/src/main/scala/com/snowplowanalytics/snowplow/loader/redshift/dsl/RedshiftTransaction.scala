package com.snowplowanalytics.snowplow.loader.redshift.dsl

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource, Timer}
import com.snowplowanalytics.snowplow.rdbloader.dsl.AWS
import com.snowplowanalytics.snowplow.loader.redshift.config.RedshiftTarget
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.Transaction
import doobie.ConnectionIO

object RedshiftTransaction {

  val RedshiftDriver = "com.amazon.redshift.jdbc42.Driver"

  def interpreter[F[_]: ConcurrentEffect: ContextShift: Timer: AWS](
    target: RedshiftTarget,
    blocker: Blocker
  ): Resource[F, Transaction[F, ConnectionIO]] = {
    val url   = s"jdbc:redshift://${target.host}:${target.port}/${target.database}"
    val props = target.jdbc.properties
    Transaction
      .buildPool[F](target.password, url, target.username, RedshiftDriver, props, blocker)
      .map(xa => Transaction.jdbcRealInterpreter[F](xa))
  }
}
