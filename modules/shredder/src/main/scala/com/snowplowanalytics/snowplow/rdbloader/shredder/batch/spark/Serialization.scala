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
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{Hierarchy, Transformed}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage._

import org.apache.spark.sql.types._

object Serialization {
  val classesToRegister: Array[Class[_]] = Array(
    classOf[Array[String]],
    classOf[Array[UUID]],
    classOf[SchemaKey],
    classOf[SelfDescribingData[_]],
    classOf[Event],
    classOf[Hierarchy],
    classOf[Instant],
    classOf[Transformed],
    classOf[Transformed.Data.DString],
    classOf[Transformed.WideRow],
    classOf[Transformed.Shredded.Json],
    classOf[Transformed.Shredded.Tabular],
    classOf[Array[Transformed]],
    classOf[UUID],
    classOf[TypesInfo.WideRow.Type],
    classOf[Array[TypesInfo.WideRow.Type]],
    classOf[SnowplowEntity],
    Class.forName("com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage$SnowplowEntity$SelfDescribingEvent$"),
    Class.forName("com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage$SnowplowEntity$Context$"),
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
    classOf[org.apache.spark.sql.execution.datasources.WriteTaskResult],
    classOf[org.apache.spark.sql.execution.datasources.ExecutedWriteSummary],
    classOf[org.apache.spark.sql.execution.datasources.BasicWriteTaskStats],
    classOf[Array[scala.util.Either[_, _]]],
    classOf[StructType],
    classOf[Array[StructType]],
    classOf[StructField],
    classOf[Array[StructField]],
    classOf[Metadata],
    classOf[IntegerType],
    classOf[DecimalType],
    classOf[ArrayType],
    Class.forName("org.apache.spark.sql.types.StringType$"),
    Class.forName("org.apache.spark.sql.types.TimestampType$"),
    Class.forName("org.apache.spark.sql.types.BooleanType$"),
    Class.forName("org.apache.spark.sql.types.IntegerType$"),
    Class.forName("org.apache.spark.sql.types.DoubleType$"),
    Class.forName("org.apache.spark.sql.types.DateType$"),
    Class.forName("org.apache.spark.sql.types.BooleanType$"),
    Class.forName("org.apache.spark.sql.types.Decimal$DecimalAsIfIntegral$"),
    Class.forName("org.apache.spark.sql.types.Decimal$DecimalIsFractional$"),
    classOf[Array[scala.runtime.BoxedUnit]]
  )
}
