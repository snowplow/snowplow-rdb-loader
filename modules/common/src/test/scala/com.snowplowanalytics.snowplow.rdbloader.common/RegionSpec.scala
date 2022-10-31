/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.common

import cats.syntax.either._

import io.circe.Decoder.{AccumulatingResult, Result}
import io.circe._

import com.snowplowanalytics.snowplow.rdbloader.common.config.Region

object RegionSpec {

  val DefaultTestRegion: Region = Region("sa-east-1")

  /**
   * Region decoder for testing. It returns predefined region if region is missing.
   */
  val testRegionConfigDecoder: Decoder[Region] = new Decoder[Region] {
    override def apply(c: HCursor): Result[Region] = tryDecode(c)

    override def tryDecode(c: ACursor): Result[Region] = c match {
      case c: HCursor =>
        if (c.value.isNull)
          Right(DefaultTestRegion)
        else
          c.value.asString match {
            case Some(r) => Region.checkRegion(Region(r)).leftMap(DecodingFailure(_, c.history))
            case None => Left(DecodingFailure("Region", c.history))
          }
      case c: FailedCursor =>
        if (!c.incorrectFocus)
          Right(DefaultTestRegion)
        else
          Left(DecodingFailure("Region", c.history))
    }

    override def decodeAccumulating(c: HCursor): AccumulatingResult[Region] =
      tryDecodeAccumulating(c)

    override def tryDecodeAccumulating(c: ACursor): AccumulatingResult[Region] =
      tryDecode(c).toValidatedNel
  }

}
