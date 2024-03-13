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

import cats.Id
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.JavaNetRegistryLookup._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.iglu.schemaddl.parquet.Type.Nullability.{Nullable, Required}
import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.SnowplowEntity
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.SnowplowEntity.{Context, SelfDescribingEvent}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.NonAtomicFieldsProvider
import io.circe.literal.JsonStringContext
import org.specs2.mutable.{Specification, Tables}

class ParquetFieldsProviderSpec extends Specification with Tables {

  import ParquetFieldsProviderSpec._

  // using com.snowplowanalytics.snowplow/test_schema/jsonSchema/... with different versions from test resources
  private val resolver = embeddedIgluClient.resolver

  "Parquet non-atomic fields provider" should {
    "produce only one field from latest type when versions are compatible" >> {
      "for contexts" in {

        "versions" | "expectedFieldName" | "expectedDdl" |>
          List((1, 0, 0)) ! "contexts_com_snowplowanalytics_snowplow_test_schema_1" ! DdlTypes.schema100 |
          List((1, 0, 0), (1, 0, 1)) ! "contexts_com_snowplowanalytics_snowplow_test_schema_1" ! DdlTypes.schema101 |
          List((1, 0, 1), (1, 1, 0)) ! "contexts_com_snowplowanalytics_snowplow_test_schema_1" ! DdlTypes.schema110 |
          List((1, 0, 0), (1, 0, 1), (1, 1, 0)) ! "contexts_com_snowplowanalytics_snowplow_test_schema_1" ! DdlTypes.schema110 |
          List((1, 0, 0), (1, 1, 0)) ! "contexts_com_snowplowanalytics_snowplow_test_schema_1" ! DdlTypes.schema110 |
          List((2, 0, 0)) ! "contexts_com_snowplowanalytics_snowplow_test_schema_2" ! DdlTypes.schema200 | {
            (versions, expectedName, expectedElementType) =>
              assertOneField(
                versions,
                entity = Context,
                expectedField = nullableArrayWithRequiredElement(expectedName, expectedElementType)
              )
          }
      }
      "for unstruct/SDE" in {

        "versions" | "expectedFieldName" | "expectedDdl" |>
          List((1, 0, 0)) ! "unstruct_event_com_snowplowanalytics_snowplow_test_schema_1" ! DdlTypes.schema100 |
          List((1, 0, 0), (1, 0, 1)) ! "unstruct_event_com_snowplowanalytics_snowplow_test_schema_1" ! DdlTypes.schema101 |
          List((1, 0, 1), (1, 1, 0)) ! "unstruct_event_com_snowplowanalytics_snowplow_test_schema_1" ! DdlTypes.schema110 |
          List((1, 0, 0), (1, 0, 1), (1, 1, 0)) ! "unstruct_event_com_snowplowanalytics_snowplow_test_schema_1" ! DdlTypes.schema110 |
          List((1, 0, 0), (1, 1, 0)) ! "unstruct_event_com_snowplowanalytics_snowplow_test_schema_1" ! DdlTypes.schema110 |
          List((2, 0, 0)) ! "unstruct_event_com_snowplowanalytics_snowplow_test_schema_2" ! DdlTypes.schema200 | {
            (versions, expectedName, expectedElementType) =>
              assertOneField(
                versions,
                entity = SelfDescribingEvent,
                expectedField = Field(expectedName, expectedElementType, nullability = Nullable)
              )
          }
      }
    }
    "collapse the two fields with colliding name(after normalization) i.e. aField and a_field" in {
      val context100 = getType(SchemaVer.Full(1, 0, 0), Context)
      val context101 = getType(SchemaVer.Full(1, 0, 1), Context)
      val inputTypes = List(context100, context101)

      val result = NonAtomicFieldsProvider.build(resolver, inputTypes).value.right.get

      result.value.size mustEqual 1
      result.value.head.field mustEqual nullableArrayWithRequiredElement(
        name = "contexts_com_snowplowanalytics_snowplow_test_schema_1",
        elementType = DdlTypes.schema101
      )
    }
    "produce two fields when" >> {
      "contexts versions are not compatible" in {
        val context100 = getType(SchemaVer.Full(1, 0, 0), Context)
        val context200 = getType(SchemaVer.Full(2, 0, 0), Context)
        val inputTypes = List(context100, context200)

        val result = NonAtomicFieldsProvider.build(resolver, inputTypes).value.right.get

        result.value.size mustEqual 2
        result.value.head.field mustEqual nullableArrayWithRequiredElement(
          name = "contexts_com_snowplowanalytics_snowplow_test_schema_1",
          elementType = DdlTypes.schema100
        )
        result.value.last.field mustEqual nullableArrayWithRequiredElement(
          name = "contexts_com_snowplowanalytics_snowplow_test_schema_2",
          elementType = DdlTypes.schema200
        )
      }

      "context and unstruct is used" in {
        val context100 = getType(SchemaVer.Full(1, 0, 0), Context)
        val unstruct100 = getType(SchemaVer.Full(1, 0, 0), SelfDescribingEvent)
        val inputTypes = List(context100, unstruct100)

        val result = NonAtomicFieldsProvider.build(resolver, inputTypes).value.right.get

        result.value.size mustEqual 2
        result.value.head.field mustEqual nullableArrayWithRequiredElement(
          name = "contexts_com_snowplowanalytics_snowplow_test_schema_1",
          elementType = DdlTypes.schema100
        )
        result.value.last.field mustEqual Field(
          name = "unstruct_event_com_snowplowanalytics_snowplow_test_schema_1",
          fieldType = DdlTypes.schema100,
          nullability = Nullable
        )
      }
    }
    "create a recovery columns with broken schema migrations" >> {
      import ParquetFieldsProviderSpec.DdlTypes._

      "schema broken from 100 to 101 to 110 should generate 3 column if all 100-101-110-111 are seen" in {
        val inputTypes = List(context100, context101, context110, context111)
        val result = NonAtomicFieldsProvider.build(resolver, inputTypes).value.right.get

        result.value.size mustEqual 3
        forall(
          result.value
            .map(s => (s.field, s.matchingKeys))
            .zip(List(expectedContext100_110, expectedContext101, expectedContext111))
        ) { case (actual, expected) => actual mustEqual expected }
      }
      "schema broken from 100 to 101 to 110 should generate 1 column if only 100 is seen" in {
        val inputTypes = List(context100)
        val result = NonAtomicFieldsProvider.build(resolver, inputTypes).value.right.get

        result.value.size mustEqual 1
        forall(
          result.value
            .map(s => (s.field, s.matchingKeys))
            .zip(List(expectedContext100))
        ) { case (actual, expected) => actual mustEqual expected }
      }

      "schema broken from 100 to 101 to 110 should generate 1 column if only 110 is seen" in {
        val inputTypes = List(context110)
        val result = NonAtomicFieldsProvider.build(resolver, inputTypes).value.right.get

        result.value.size mustEqual 1
        forall(
          result.value
            .map(s => (s.field, s.matchingKeys))
            .zip(List(expectedContext100_110))
        ) { case (actual, expected) => actual mustEqual expected }
      }
    }
  }

  private def assertOneField(
    inputTypesVersions: List[(Int, Int, Int)],
    entity: SnowplowEntity,
    expectedField: Field
  ) = {
    val inputTypes = inputTypesVersions.map { case (model, revision, addition) =>
      getType(SchemaVer.Full(model, revision, addition), entity)
    }
    val result = NonAtomicFieldsProvider.build(resolver, inputTypes).value.right.get

    result.value.size mustEqual 1
    result.value.head.field mustEqual expectedField
  }

}

object ParquetFieldsProviderSpec {
  private def getType(version: SchemaVer.Full, entity: SnowplowEntity) =
    WideRow.Type(SchemaKey(vendor = "com.snowplowanalytics.snowplow", name = "test_schema", format = "jsonschema", version), entity)

  private def getBrokenType(version: SchemaVer.Full, entity: SnowplowEntity) =
    WideRow.Type(SchemaKey(vendor = "com.snowplowanalytics.snowplow", name = "test_schema_broken", format = "jsonschema", version), entity)

  private def nullableArrayWithRequiredElement(
    name: String,
    elementType: Type,
    accessors: Set[String] = Set.empty[String]
  ) =
    Field(
      name,
      fieldType = Type.Array(elementType, nullability = Required),
      nullability = Nullable,
      if (accessors.isEmpty) Set(name) else accessors
    )

  val igluConfig =
    json"""
        {
          "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
          "data": {
            "cacheSize": 500,
            "repositories": [
              {
               "name": "Iglu Embedded",
               "priority": 0,
               "vendorPrefixes": [ "com.snowplowanalytics" ],
               "connection": {
                 "embedded": {
                   "path": "/"
                 }
               }
             }
            ]
          }
        }"""

  val embeddedIgluClient = Client.parseDefault[Id](igluConfig).value.right.get

  object DdlTypes {

    val schema100 = Type.Struct(
      fields = List(
        Field(
          "a_field",
          Type.Struct(
            List(
              Field("b_field", Type.String, Nullable),
              Field(
                "c_field",
                Type.Struct(
                  List(
                    Field("d_field", Type.String, Nullable)
                  )
                ),
                Nullable
              )
            )
          ),
          Required
        ),
        Field("b_field", Type.Long, Required),
        Field("d_field", Type.Double, Nullable),
        Field("e_field", Type.Json, Nullable),
        Field("g_field", Type.String, Nullable),
        Field("h_field", Type.Timestamp, Nullable),
        Field(
          "i_field",
          Type.Array(
            Type.Struct(
              List(
                Field("c_field", Type.Long, Nullable),
                Field("d_field", Type.String, Nullable)
              )
            ),
            Required
          ),
          Nullable
        )
      )
    )
    val schema101 = Type.Struct(
      fields = List(
        Field(
          "a_field",
          Type.Struct(
            List(
              Field("b_field", Type.String, Nullable),
              Field(
                "c_field",
                Type.Struct(
                  List(
                    Field("d_field", Type.String, Nullable),
                    Field("e_field", Type.String, Nullable)
                  )
                ),
                Nullable
              )
            )
          ),
          Required
        ),
        Field("b_field", Type.Long, Required),
        Field("c_field", Type.Boolean, Nullable),
        Field("d_field", Type.Double, Nullable),
        Field("e_field", Type.Json, Nullable),
        Field("g_field", Type.String, Nullable),
        Field("h_field", Type.Timestamp, Nullable),
        Field(
          "i_field",
          Type.Array(
            Type.Struct(
              List(
                Field("c_field", Type.Long, Nullable),
                Field("d_field", Type.String, Nullable)
              )
            ),
            Required
          ),
          Nullable
        )
      )
    )
    val schema110 = Type.Struct(
      fields = List(
        Field(
          "a_field",
          Type.Struct(
            List(
              Field("b_field", Type.String, Nullable),
              Field(
                "c_field",
                Type.Struct(
                  List(
                    Field("d_field", Type.String, Nullable),
                    Field("e_field", Type.String, Nullable)
                  )
                ),
                Nullable
              ),
              Field("d_field", Type.String, Nullable)
            )
          ),
          Required
        ),
        Field("b_field", Type.Long, Required),
        Field("c_field", Type.Boolean, Nullable),
        Field("d_field", Type.Double, Nullable),
        Field("e_field", Type.Json, Nullable),
        Field("f_field", Type.Json, Nullable),
        Field("g_field", Type.String, Nullable),
        Field("h_field", Type.Timestamp, Nullable),
        Field(
          "i_field",
          Type.Array(
            Type.Struct(
              List(
                Field("c_field", Type.Long, Nullable),
                Field("d_field", Type.String, Nullable)
              )
            ),
            Required
          ),
          Nullable
        )
      )
    )

    val schema200 = Type.Struct(
      fields = List(
        Field("a_field", Type.String, Required),
        Field("e_field", Type.String, Required),
        Field("f_field", Type.Long, Required)
      )
    )

    val brokenSchema100 = Type.Struct(fields = List(Field("b_field", Type.Long, Nullable)))
    val brokenSchema101 = Type.Struct(fields = List(Field("b_field", Type.String, Nullable)))
    val brokenSchema110 = Type.Struct(fields =
      List(
        Field("a_field", Type.Long, Nullable),
        Field("b_field", Type.Long, Nullable)
      )
    )
    val brokenSchema111 = Type.Struct(fields = List(Field("a_field", Type.String, Nullable)))

    val context100 = getBrokenType(SchemaVer.Full(1, 0, 0), Context)
    val context101 = getBrokenType(SchemaVer.Full(1, 0, 1), Context) // breaking
    val context110 = getBrokenType(SchemaVer.Full(1, 1, 0), Context) // 1-1-0 is compatible with 1-0-0
    val context111 = getBrokenType(SchemaVer.Full(1, 1, 1), Context) // breaking

    val expectedContext100 = (
      nullableArrayWithRequiredElement(
        name = "contexts_com_snowplowanalytics_snowplow_test_schema_broken_1",
        elementType = DdlTypes.brokenSchema100
      ),
      Set(context100.schemaKey)
    )
    val expectedContext100_110 = (
      nullableArrayWithRequiredElement(
        name = "contexts_com_snowplowanalytics_snowplow_test_schema_broken_1",
        elementType = DdlTypes.brokenSchema110
      ),
      Set(context100.schemaKey, context110.schemaKey)
    )
    val expectedContext111 = (
      nullableArrayWithRequiredElement(
        name = "contexts_com_snowplowanalytics_snowplow_test_schema_broken_1_recovered_1_1_1_737559706",
        elementType = DdlTypes.brokenSchema111,
        accessors = Set("contexts_com_snowplowanalytics_snowplow_test_schema_broken_1")
      ),
      Set(context111.schemaKey)
    )
    val expectedContext101 = (
      nullableArrayWithRequiredElement(
        name = "contexts_com_snowplowanalytics_snowplow_test_schema_broken_1_recovered_1_0_1_1837344102",
        elementType = DdlTypes.brokenSchema101,
        accessors = Set("contexts_com_snowplowanalytics_snowplow_test_schema_broken_1")
      ),
      Set(context101.schemaKey)
    )
  }
}
