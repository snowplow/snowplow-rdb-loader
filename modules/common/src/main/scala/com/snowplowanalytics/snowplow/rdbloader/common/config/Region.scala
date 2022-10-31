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
package com.snowplowanalytics.snowplow.rdbloader.common.config

import scala.jdk.CollectionConverters._

import cats.syntax.either._

import io.circe.Decoder.{AccumulatingResult, Result}
import io.circe._

import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import software.amazon.awssdk.regions.{Region => AWSRegion}

final case class Region(name: String) extends AnyVal
object Region {

  private val regionResolveErrorMessage = "Region cannot be resolved, needs to be passed explicitly"

  /**
   * Custom Region decoder which allows Region types to be missing. If they are missing, it tries to
   * retrieve region with "DefaultAwsRegionProviderChain". If it is also unsuccessful, it throws
   * error. In the tests, it is changed with dummy config reader in order to not use
   * "DefaultAwsRegionProviderChain" during tests.
   */
  val regionConfigDecoder: Decoder[Region] = new Decoder[Region] {
    override def apply(c: HCursor): Result[Region] = tryDecode(c)

    override def tryDecode(c: ACursor): Result[Region] = c match {
      case c: HCursor =>
        if (c.value.isNull)
          getRegion.toRight(DecodingFailure(regionResolveErrorMessage, c.history))
        else
          c.value.asString match {
            case Some(r) => checkRegion(Region(r)).leftMap(DecodingFailure(_, c.history))
            case None => Left(DecodingFailure("Region", c.history))
          }
      case c: FailedCursor =>
        if (!c.incorrectFocus)
          getRegion.toRight(DecodingFailure(regionResolveErrorMessage, c.history))
        else
          Left(DecodingFailure("Region", c.history))
    }

    override def decodeAccumulating(c: HCursor): AccumulatingResult[Region] =
      tryDecodeAccumulating(c)

    override def tryDecodeAccumulating(c: ACursor): AccumulatingResult[Region] =
      tryDecode(c).toValidatedNel
  }

  private def getRegion: Option[Region] =
    Either.catchNonFatal((new DefaultAwsRegionProviderChain).getRegion).toOption.map(r => Region(r.id()))

  def checkRegion(region: Region): Either[String, Region] = {
    val allRegions = AWSRegion.regions().asScala.map(_.id())
    if (allRegions.contains(region.name)) region.asRight
    else s"Region ${region.name} is unknown, choose from [${allRegions.mkString(", ")}]".asLeft
  }
}
