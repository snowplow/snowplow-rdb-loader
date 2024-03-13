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
