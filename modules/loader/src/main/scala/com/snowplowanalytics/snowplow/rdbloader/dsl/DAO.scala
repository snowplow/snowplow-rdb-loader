/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader.dsl

import doobie.{ConnectionIO, HPS, HRS, Read}

import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.db.{Statement, Target}

/**
 * An effect declaration of communicating with a DB. Typically, it represents a second effect from
 * the main app, i.e. communication with DB happens within `C` effect and then it's translated with
 * [[Transaction]] into `F`.
 *
 * This is done mostly to mimic doobie's `ConnectionIO` behavior, which is is a separate effect (not
 * `IO`). Also we cannot have `ConnectionIO` anywhere in tests as it's impossible to inspect
 */
trait DAO[C[_]] {

  /** Execute single SQL statement */
  def executeUpdate(sql: Statement, purpose: DAO.Purpose): C[Int]

  /** Execute query and parse results into `A` */
  def executeQuery[A](query: Statement)(implicit A: Read[A]): C[A]

  /** Execute query and parse results into 0 or more `A`s */
  def executeQueryList[A](query: Statement)(implicit A: Read[A]): C[List[A]]
}

object DAO {

  sealed trait Purpose
  object Purpose {
    case object Loading extends Purpose
    case object NonLoading extends Purpose
  }

  def apply[F[_]](implicit ev: DAO[F]): DAO[F] = ev

  def connectionIO[I](dbTarget: Target[I], timeouts: Config.Timeouts): DAO[ConnectionIO] = new DAO[ConnectionIO] {

    /** Execute single SQL statement (against target in interpreter) */
    def executeUpdate(sql: Statement, purpose: Purpose): ConnectionIO[Int] = {
      val timeout = purpose match {
        case Purpose.Loading => timeouts.loading
        case Purpose.NonLoading => timeouts.nonLoading
      }
      dbTarget.toFragment(sql).execWith {
        HPS.setQueryTimeout(timeout.toSeconds.toInt).flatMap(_ => HPS.executeUpdate)
      }
    }

    /** Execute query and parse results into `A` */
    def executeQuery[A](query: Statement)(implicit A: Read[A]): ConnectionIO[A] =
      dbTarget.toFragment(query).execWith {
        HPS.setQueryTimeout(timeouts.nonLoading.toSeconds.toInt).flatMap(_ => HPS.executeQuery(HRS.getUnique))
      }

    def executeQueryList[A](query: Statement)(implicit A: Read[A]): ConnectionIO[List[A]] =
      dbTarget.toFragment(query).execWith {
        HPS.setQueryTimeout(timeouts.nonLoading.toSeconds.toInt).flatMap(_ => HPS.executeQuery(HRS.build))
      }
  }
}
