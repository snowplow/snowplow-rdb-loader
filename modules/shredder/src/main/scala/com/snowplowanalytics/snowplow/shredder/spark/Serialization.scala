package com.snowplowanalytics.snowplow.shredder.spark

import java.util.UUID
import java.time.Instant

import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.shredder.transformation.{FinalRow, Hierarchy}

object Serialization {
  val classesToRegister: Array[Class[_]] = Array(
    classOf[Array[String]],
    classOf[Array[UUID]],
    classOf[SchemaKey],
    classOf[SelfDescribingData[_]],
    classOf[Event],
    classOf[Hierarchy],
    classOf[FinalRow],
    classOf[Instant],
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
    classOf[java.util.LinkedHashMap[_, _]],
    classOf[java.util.ArrayList[_]],
    Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
    Class.forName("scala.math.Ordering$Reverse"),
    classOf[org.apache.spark.sql.catalyst.InternalRow],
    Class.forName("com.snowplowanalytics.snowplow.shredder.transformation.EventUtils$$anonfun$1"),  // Ordering
    classOf[org.apache.spark.sql.execution.datasources.WriteTaskResult],
    classOf[org.apache.spark.sql.execution.datasources.ExecutedWriteSummary],
    classOf[org.apache.spark.sql.execution.datasources.BasicWriteTaskStats]
  )
}
