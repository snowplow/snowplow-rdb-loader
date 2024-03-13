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
package com.snowplowanalytics.snowplow.rdbloader.loading

import cats.Show
import cats.syntax.show._

/**
 * Loading stage. Represents the finite state machine of the Loader, it can be only in one of the
 * below stages Internal state sets the stage right before it gets executed, i.e. if it failed being
 * in `ManifestCheck` stage it means that manifest check has failed, but it certainly has started
 */
sealed trait Stage

object Stage {

  /** Figure out how the migration should look like, by inspecting affected tables. First stage */
  final case object MigrationBuild extends Stage

  /** Pre-transaction migrations, such as ALTER COLUMN. Usually empty. Second stage */
  final case object MigrationPre extends Stage

  /** Checking manifest if the folder is already loaded. Third stage */
  final case object ManifestCheck extends Stage

  /** In-transaction migrations, such as ADD COLUMN. Fourth stage */
  final case object MigrationIn extends Stage

  /** Actual loading into a table. Appears for many different tables. Fifth stage */
  final case class Loading(table: String) extends Stage

  /** Adding manifest item, acking SQS comment. Sixth stage */
  final case object Committing extends Stage

  /** Abort the loading. Can appear after any stage */
  final case class Cancelling(reason: String) extends Stage

  implicit val stageShow: Show[Stage] =
    Show.show {
      case MigrationBuild => "migration building"
      case MigrationPre => "pre-transaction migrations"
      case ManifestCheck => "manifest check"
      case MigrationIn => "in-transaction migrations"
      case Loading(table) => show"copying into $table table"
      case Committing => "committing"
      case Cancelling(reason) => show"cancelling because of $reason"
    }
}
