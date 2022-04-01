/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import cats.Monad
import cats.data.EitherT
import cats.implicits._
import cats.effect.Clock

import com.snowplowanalytics.iglu.schemaddl.bigquery.{Field, Mode, Type}
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.WideField

import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DateType, DecimalType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

object SparkSchema {

  val CustomDecimal = DecimalType(18, 2)

  def build[F[_]: Clock: Monad: RegistryLookup](resolver: Resolver[F], types: List[LoaderMessage.TypesInfo.WideRow.Type]): F[Either[String, StructType]] = {
    val latestByModel = LoaderMessage.TypesInfo.WideRow.latestByModel(types)
    latestByModel.sorted
      .traverse(forEntity(resolver, _))
      .map(forEntities => StructType(Atomic ::: forEntities))
      .value
  }

  def forEntity[F[_]: Clock: Monad: RegistryLookup](resolver: Resolver[F], entity: LoaderMessage.TypesInfo.WideRow.Type): EitherT[F, String, StructField] =
    WideField.getSchema(resolver, entity.schemaKey).map { schema =>
      val name = SnowplowEvent.transformSchema(entity.snowplowEntity.toSdkProperty, entity.schemaKey)
      entity.snowplowEntity match {
        case LoaderMessage.SnowplowEntity.SelfDescribingEvent =>
          // TODO: Should 'Field.normalized' be called here ?
          val ddlField = Field.build(name, schema, false)
          structField(ddlField)
        case LoaderMessage.SnowplowEntity.Context =>
          // TODO: Should 'Field.normalized' be called here ?
          val ddlField = Field.build("NOT_NEEDED", schema, true)
          StructField(name, new ArrayType(fieldType(ddlField.fieldType), false), true)
      }
    }.leftMap(_.toString)

  def structField(ddlField: Field): StructField =
    ddlField match {
      case Field(name, ddlType, Mode.Nullable) =>
        StructField(name, fieldType(ddlType), true)
      case Field(name, ddlType, Mode.Required) =>
        StructField(name, fieldType(ddlType), false)
      case Field(name, ddlType, Mode.Repeated) =>
        val itemIsNullable = true // TODO: We don't have this information!  Because BQ does not support nullable array items
        val arrayIsNullable = true // TODO: We don't have this information!  Because BQ does not support nullable arrays.
        StructField(name, new ArrayType(fieldType(ddlType), itemIsNullable), arrayIsNullable)
    }

  def fieldType(ddlType: Type): DataType = ddlType match {
    case Type.String => StringType
    case Type.Boolean => BooleanType
    case Type.Integer => IntegerType
    case Type.Float => DoubleType // TODO: Bigquery ddl does not make distinction between float and double.
    case Type.Numeric => DoubleType // TODO: Bigquery ddl never emits this type.  Would be better if it did, and we could use Spark's DecimalType
    case Type.Date => DateType
    case Type.DateTime => TimestampType
    case Type.Timestamp => TimestampType
    case Type.Record(fields) => StructType(fields.map(structField))
  }

  val Atomic = List(
    StructField("app_id",                   StringType,     true),
    StructField("platform",                 StringType,     true),
    StructField("etl_tstamp",               TimestampType,  true),
    StructField("collector_tstamp",         TimestampType,  false),
    StructField("dvce_created_tstamp",      TimestampType,  true),
    StructField("event",                    StringType,     true),
    StructField("event_id",                 StringType,     false),
    StructField("txn_id",                   IntegerType,    true),
    StructField("name_tracker",             StringType,     true),
    StructField("v_tracker",                StringType,     true),
    StructField("v_collector",              StringType,     false),
    StructField("v_etl",                    StringType,     false),
    StructField("user_id",                  StringType,     true),
    StructField("user_ipaddress",           StringType,     true),
    StructField("user_fingerprint",         StringType,     true),
    StructField("domain_userid",            StringType,     true),
    StructField("domain_sessionidx",        IntegerType,    true),
    StructField("network_userid",           StringType,     true),
    StructField("geo_country",              StringType,     true),
    StructField("geo_region",               StringType,     true),
    StructField("geo_city",                 StringType,     true),
    StructField("geo_zipcode",              StringType,     true),
    StructField("geo_latitude",             DoubleType,     true),
    StructField("geo_longitude",            DoubleType,     true),
    StructField("geo_region_name",          StringType,     true),
    StructField("ip_isp",                   StringType,     true),
    StructField("ip_organization",          StringType,     true),
    StructField("ip_domain",                StringType,     true),
    StructField("ip_netspeed",              StringType,     true),
    StructField("page_url",                 StringType,     true),
    StructField("page_title",               StringType,     true),
    StructField("page_referrer",            StringType,     true),
    StructField("page_urlscheme",           StringType,     true),
    StructField("page_urlhost",             StringType,     true),
    StructField("page_urlport",             IntegerType,    true),
    StructField("page_urlpath",             StringType,     true),
    StructField("page_urlquery",            StringType,     true),
    StructField("page_urlfragment",         StringType,     true),
    StructField("refr_urlscheme",           StringType,     true),
    StructField("refr_urlhost",             StringType,     true),
    StructField("refr_urlport",             IntegerType,    true),
    StructField("refr_urlpath",             StringType,     true),
    StructField("refr_urlquery",            StringType,     true),
    StructField("refr_urlfragment",         StringType,     true),
    StructField("refr_medium",              StringType,     true),
    StructField("refr_source",              StringType,     true),
    StructField("refr_term",                StringType,     true),
    StructField("mkt_medium",               StringType,     true),
    StructField("mkt_source",               StringType,     true),
    StructField("mkt_term",                 StringType,     true),
    StructField("mkt_content",              StringType,     true),
    StructField("mkt_campaign",             StringType,     true),
    StructField("se_category",              StringType,     true),
    StructField("se_action",                StringType,     true),
    StructField("se_label",                 StringType,     true),
    StructField("se_property",              StringType,     true),
    StructField("se_value",                 DoubleType,     true),
    StructField("tr_orderid",               StringType,     true),
    StructField("tr_affiliation",           StringType,     true),
    StructField("tr_total",                 CustomDecimal,  true),
    StructField("tr_tax",                   CustomDecimal,  true),
    StructField("tr_shipping",              CustomDecimal,  true),
    StructField("tr_city",                  StringType,     true),
    StructField("tr_state",                 StringType,     true),
    StructField("tr_country",               StringType,     true),
    StructField("ti_orderid",               StringType,     true),
    StructField("ti_sku",                   StringType,     true),
    StructField("ti_name",                  StringType,     true),
    StructField("ti_category",              StringType,     true),
    StructField("ti_price",                 CustomDecimal,  true),
    StructField("ti_quantity",              IntegerType,    true),
    StructField("pp_xoffset_min",           IntegerType,    true),
    StructField("pp_xoffset_max",           IntegerType,    true),
    StructField("pp_yoffset_min",           IntegerType,    true),
    StructField("pp_yoffset_max",           IntegerType,    true),
    StructField("useragent",                StringType,     true),
    StructField("br_name",                  StringType,     true),
    StructField("br_family",                StringType,     true),
    StructField("br_version",               StringType,     true),
    StructField("br_type",                  StringType,     true),
    StructField("br_renderengine",          StringType,     true),
    StructField("br_lang",                  StringType,     true),
    StructField("br_features_pdf",          BooleanType,    true),
    StructField("br_features_flash",        BooleanType,    true),
    StructField("br_features_java",         BooleanType,    true),
    StructField("br_features_director",     BooleanType,    true),
    StructField("br_features_quicktime",    BooleanType,    true),
    StructField("br_features_realplayer",   BooleanType,    true),
    StructField("br_features_windowsmedia", BooleanType,    true),
    StructField("br_features_gears",        BooleanType,    true),
    StructField("br_features_silverlight",  BooleanType,    true),
    StructField("br_cookies",               BooleanType,    true),
    StructField("br_colordepth",            StringType,     true),
    StructField("br_viewwidth",             IntegerType,    true),
    StructField("br_viewheight",            IntegerType,    true),
    StructField("os_name",                  StringType,     true),
    StructField("os_family",                StringType,     true),
    StructField("os_manufacturer",          StringType,     true),
    StructField("os_timezone",              StringType,     true),
    StructField("dvce_type",                StringType,     true),
    StructField("dvce_ismobile",            BooleanType,    true),
    StructField("dvce_screenwidth",         IntegerType,    true),
    StructField("dvce_screenheight",        IntegerType,    true),
    StructField("doc_charset",              StringType,     true),
    StructField("doc_width",                IntegerType,    true),
    StructField("doc_height",               IntegerType,    true),
    StructField("tr_currency",              StringType,     true),
    StructField("tr_total_base",            CustomDecimal,  true),
    StructField("tr_tax_base",              CustomDecimal,  true),
    StructField("tr_shipping_base",         CustomDecimal,  true),
    StructField("ti_currency",              StringType,     true),
    StructField("ti_price_base",            CustomDecimal,  true),
    StructField("base_currency",            StringType,     true),
    StructField("geo_timezone",             StringType,     true),
    StructField("mkt_clickid",              StringType,     true),
    StructField("mkt_network",              StringType,     true),
    StructField("etl_tags",                 StringType,     true),
    StructField("dvce_sent_tstamp",         TimestampType,  true),
    StructField("refr_domain_userid",       StringType,     true),
    StructField("refr_dvce_tstamp",         TimestampType,  true),
    StructField("domain_sessionid",         StringType,     true),
    StructField("derived_tstamp",           TimestampType,  true),
    StructField("event_vendor",             StringType,     true),
    StructField("event_name",               StringType,     true),
    StructField("event_format",             StringType,     true),
    StructField("event_version",            StringType,     true),
    StructField("event_fingerprint",        StringType,     true),
    StructField("true_tstamp",              TimestampType,  true)
  )
}
