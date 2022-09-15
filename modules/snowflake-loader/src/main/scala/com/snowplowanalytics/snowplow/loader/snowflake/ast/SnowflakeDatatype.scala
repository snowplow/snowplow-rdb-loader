/*
 * Copyright (c) 2022-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
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
        case Some(s) =>  Fragment.const0(s"VARCHAR($s)")
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

