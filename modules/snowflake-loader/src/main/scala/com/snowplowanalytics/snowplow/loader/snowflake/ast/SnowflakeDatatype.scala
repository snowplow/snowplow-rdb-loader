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
package com.snowplowanalytics.snowplow.loader.snowflake.ast

import doobie.Fragment
import doobie.implicits._

sealed trait SnowflakeDatatype extends Ddl

object SnowflakeDatatype {
  final case class Varchar(size: Option[Int]) extends SnowflakeDatatype {
    def toDdl: Fragment =
      size match {
        case None => fr0"VARCHAR"
        case Some(s) => Fragment.const0(s"VARCHAR($s)")
      }
  }
  object Varchar {
    def apply(): Varchar = Varchar(None)
    def apply(size: Int): Varchar = Varchar(Some(size))
  }

  final case object Timestamp extends SnowflakeDatatype {
    def toDdl: Fragment = fr0"TIMESTAMP_NTZ"
  }

  final case class Char(size: Int) extends SnowflakeDatatype {
    def toDdl: Fragment = Fragment.const0(s"CHAR($size)")
  }

  final case object SmallInt extends SnowflakeDatatype {
    def toDdl: Fragment = fr0"SMALLINT"
  }

  final case object DoublePrecision extends SnowflakeDatatype {
    def toDdl: Fragment = fr0"DOUBLE PRECISION"
  }

  final case object Integer extends SnowflakeDatatype {
    def toDdl: Fragment = fr0"INTEGER"
  }

  final case class Number(precision: Int, scale: Int) extends SnowflakeDatatype {
    def toDdl: Fragment = Fragment.const0(s"NUMBER($precision,$scale)")
  }

  final case object Boolean extends SnowflakeDatatype {
    def toDdl: Fragment = fr0"BOOLEAN"
  }

  final case object Variant extends SnowflakeDatatype {
    def toDdl: Fragment = fr0"VARIANT"
  }

  final case object JsonObject extends SnowflakeDatatype {
    def toDdl: Fragment = fr0"OBJECT"
  }

  final case object JsonArray extends SnowflakeDatatype {
    def toDdl: Fragment = fr0"ARRAY"
  }
}
