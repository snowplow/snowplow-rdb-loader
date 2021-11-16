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
package com.snowplowanalytics.snowplow.rdbloader.shredder.batch.spark

import java.util.UUID
import java.time.Instant

import com.snowplowanalytics.iglu.core.{SelfDescribingData, SchemaKey}

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{Hierarchy, Shredded}

object Serialization {
  val classesToRegister: Array[Class[_]] = Array(
    classOf[Array[String]],
    classOf[Array[UUID]],
    classOf[SchemaKey],
    classOf[SelfDescribingData[_]],
    classOf[Event],
    classOf[Hierarchy],
    classOf[Instant],
    classOf[Array[Shredded]],
    classOf[Shredded.Tabular],
    classOf[UUID],
    Class.forName("com.snowplowanalytics.iglu.core.SchemaVer$Full"),
    Class.forName("io.circe.JsonObject$LinkedHashMapJsonObject"),
    Class.forName("io.circe.Json$JObject"),
    Class.forName("io.circe.Json$JString"),
    Class.forName("io.circe.Json$JArray"),
    Class.forName("io.circe.Json$JNull$"),
    Class.forName("io.circe.Json$JNumber"),
    Class.forName("io.circe.Json$JBoolean"),
    classOf[io.circe.Json],
    Class.forName("io.circe.JsonLong"),
    Class.forName("io.circe.JsonDecimal"),
    Class.forName("io.circe.JsonBigDecimal"),
    Class.forName("io.circe.JsonBiggerDecimal"),
    Class.forName("io.circe.JsonDouble"),
    Class.forName("io.circe.JsonFloat"),
    Class.forName("io.circe.numbers.SigAndExp"),
    Class.forName("io.circe.numbers.BiggerDecimal$$anon$1"),
    classOf[java.util.LinkedHashMap[_, _]],
    classOf[java.util.ArrayList[_]],
    classOf[java.math.BigInteger],
    classOf[java.math.BigDecimal],
    Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
    Class.forName("scala.math.Ordering$Reverse"),
    classOf[org.apache.spark.sql.catalyst.InternalRow],
    Class.forName("com.snowplowanalytics.snowplow.rdbloader.common.transformation.EventUtils$$anonfun$1"),  // Ordering
    Class.forName("com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage$Format$TSV$"),
    classOf[org.apache.spark.sql.execution.datasources.WriteTaskResult],
    classOf[org.apache.spark.sql.execution.datasources.ExecutedWriteSummary],
    classOf[org.apache.spark.sql.execution.datasources.BasicWriteTaskStats],
    classOf[Array[scala.util.Either[_, _]]],
    classOf[Array[scala.runtime.BoxedUnit]]
  )
}
