/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.shredder.transformation

import cats.implicits._

import io.circe.Json

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.BadRow

import com.snowplowanalytics.snowplow.shredder.spark.singleton

import org.apache.spark.sql.Row

/** Final stage of event. After this, it can be shredded into different folders */
case class FinalRow(atomic: Row, shredded: List[Shredded])

object FinalRow {

  def shred(igluConfig: Json, isTabular: SchemaKey => Boolean, atomicLengths: Map[String, Int])(event: Event): Either[BadRow, FinalRow] =
    Hierarchy.fromEvent(event).traverse { hierarchy =>
      val tabular = isTabular(hierarchy.entity.schema)
      Shredded.fromHierarchy(tabular, singleton.IgluSingleton.get(igluConfig).resolver)(hierarchy).toValidatedNel
    }.leftMap(EventUtils.shreddingBadRow(event)).toEither.map { shredded =>
      val row = Row(EventUtils.alterEnrichedEvent(event, atomicLengths))
      FinalRow(row, shredded)
    }
}