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
package com.snowplowanalytics.snowplow.rdbloader.shredder.batch

import cats.Id
import cats.data.Validated
import cats.implicits._

import io.circe.Json

import org.apache.spark.sql.types.{StringType, StructField, StructType, DoubleType, IntegerType, BooleanType, TimestampType}

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.snowplow.rdbloader.common.catsClockIdInstance
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Flattening
import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.spark.singleton
import com.snowplowanalytics.snowplow.rdbloader.common.Common
import com.snowplowanalytics.snowplow.badrows.FailureDetails

object SparkSchema {
  val TypeMap = Map(
    "app_id"                   -> (("StringType",     true)),
    "platform"                 -> (("StringType",     true)),
    "etl_tstamp"               -> (("TimestampType",  true)),
    "collector_tstamp"         -> (("TimestampType",  false)),
    "dvce_created_tstamp"      -> (("TimestampType",  true)),
    "event"                    -> (("StringType",     true)),
    "event_id"                 -> (("StringType",     false)),
    "txn_id"                   -> (("StringType",    true)),
    "name_tracker"             -> (("StringType",     true)),
    "v_tracker"                -> (("StringType",     true)),
    "v_collector"              -> (("StringType",     false)),
    "v_etl"                    -> (("StringType",     false)),
    "user_id"                  -> (("StringType",     true)),
    "user_ipaddress"           -> (("StringType",     true)),
    "user_fingerprint"         -> (("StringType",     true)),
    "domain_userid"            -> (("StringType",     true)),
    "domain_sessionidx"        -> (("StringType",    true)),
    "network_userid"           -> (("StringType",     true)),
    "geo_country"              -> (("StringType",     true)),
    "geo_region"               -> (("StringType",     true)),
    "geo_city"                 -> (("StringType",     true)),
    "geo_zipcode"              -> (("StringType",     true)),
    "geo_latitude"             -> (("StringType",     true)),
    "geo_longitude"            -> (("StringType",     true)),
    "geo_region_name"          -> (("StringType",     true)),
    "ip_isp"                   -> (("StringType",     true)),
    "ip_organization"          -> (("StringType",     true)),
    "ip_domain"                -> (("StringType",     true)),
    "ip_netspeed"              -> (("StringType",     true)),
    "page_url"                 -> (("StringType",     true)),
    "page_title"               -> (("StringType",     true)),
    "page_referrer"            -> (("StringType",     true)),
    "page_urlscheme"           -> (("StringType",     true)),
    "page_urlhost"             -> (("StringType",     true)),
    "page_urlport"             -> (("StringType",    true)),
    "page_urlpath"             -> (("StringType",     true)),
    "page_urlquery"            -> (("StringType",     true)),
    "page_urlfragment"         -> (("StringType",     true)),
    "refr_urlscheme"           -> (("StringType",     true)),
    "refr_urlhost"             -> (("StringType",     true)),
    "refr_urlport"             -> (("StringType",    true)),
    "refr_urlpath"             -> (("StringType",     true)),
    "refr_urlquery"            -> (("StringType",     true)),
    "refr_urlfragment"         -> (("StringType",     true)),
    "refr_medium"              -> (("StringType",     true)),
    "refr_source"              -> (("StringType",     true)),
    "refr_term"                -> (("StringType",     true)),
    "mkt_medium"               -> (("StringType",     true)),
    "mkt_source"               -> (("StringType",     true)),
    "mkt_term"                 -> (("StringType",     true)),
    "mkt_content"              -> (("StringType",     true)),
    "mkt_campaign"             -> (("StringType",     true)),
    "se_category"              -> (("StringType",     true)),
    "se_action"                -> (("StringType",     true)),
    "se_label"                 -> (("StringType",     true)),
    "se_property"              -> (("StringType",     true)),
    "se_value"                 -> (("StringType",     true)),
    "tr_orderid"               -> (("StringType",     true)),
    "tr_affiliation"           -> (("StringType",     true)),
    "tr_total"                 -> (("StringType",     true)),
    "tr_tax"                   -> (("StringType",     true)),
    "tr_shipping"              -> (("StringType",     true)),
    "tr_city"                  -> (("StringType",     true)),
    "tr_state"                 -> (("StringType",     true)),
    "tr_country"               -> (("StringType",     true)),
    "ti_orderid"               -> (("StringType",     true)),
    "ti_sku"                   -> (("StringType",     true)),
    "ti_name"                  -> (("StringType",     true)),
    "ti_category"              -> (("StringType",     true)),
    "ti_price"                 -> (("StringType",     true)),
    "ti_quantity"              -> (("StringType",    true)),
    "pp_xoffset_min"           -> (("StringType",    true)),
    "pp_xoffset_max"           -> (("StringType",    true)),
    "pp_yoffset_min"           -> (("StringType",    true)),
    "pp_yoffset_max"           -> (("StringType",    true)),
    "useragent"                -> (("StringType",     true)),
    "br_name"                  -> (("StringType",     true)),
    "br_family"                -> (("StringType",     true)),
    "br_version"               -> (("StringType",     true)),
    "br_type"                  -> (("StringType",     true)),
    "br_renderengine"          -> (("StringType",     true)),
    "br_lang"                  -> (("StringType",     true)),
    "br_features_pdf"          -> (("StringType",    true)),
    "br_features_flash"        -> (("StringType",    true)),
    "br_features_java"         -> (("StringType",    true)),
    "br_features_director"     -> (("StringType",    true)),
    "br_features_quicktime"    -> (("StringType",    true)),
    "br_features_realplayer"   -> (("StringType",    true)),
    "br_features_windowsmedia" -> (("StringType",    true)),
    "br_features_gears"        -> (("StringType",    true)),
    "br_features_silverlight"  -> (("StringType",    true)),
    "br_cookies"               -> (("StringType",    true)),
    "br_colordepth"            -> (("StringType",     true)),
    "br_viewwidth"             -> (("StringType",    true)),
    "br_viewheight"            -> (("StringType",    true)),
    "os_name"                  -> (("StringType",     true)),
    "os_family"                -> (("StringType",     true)),
    "os_manufacturer"          -> (("StringType",     true)),
    "os_timezone"              -> (("StringType",     true)),
    "dvce_type"                -> (("StringType",     true)),
    "dvce_ismobile"            -> (("StringType",    true)),
    "dvce_screenwidth"         -> (("StringType",    true)),
    "dvce_screenheight"        -> (("StringType",    true)),
    "doc_charset"              -> (("StringType",     true)),
    "doc_width"                -> (("StringType",    true)),
    "doc_height"               -> (("StringType",    true)),
    "tr_currency"              -> (("StringType",     true)),
    "tr_total_base"            -> (("StringType",     true)),
    "tr_tax_base"              -> (("StringType",     true)),
    "tr_shipping_base"         -> (("StringType",     true)),
    "ti_currency"              -> (("StringType",     true)),
    "ti_price_base"            -> (("StringType",     true)),
    "base_currency"            -> (("StringType",     true)),
    "geo_timezone"             -> (("StringType",     true)),
    "mkt_clickid"              -> (("StringType",     true)),
    "mkt_network"              -> (("StringType",     true)),
    "etl_tags"                 -> (("StringType",     true)),
    "dvce_sent_tstamp"         -> (("StringType",  true)),
    "refr_domain_userid"       -> (("StringType",     true)),
    "refr_dvce_tstamp"         -> (("StringType",  true)),
    "domain_sessionid"         -> (("StringType",     true)),
    "derived_tstamp"           -> (("StringType",  true)),
    "event_vendor"             -> (("StringType",     true)),
    "event_name"               -> (("StringType",     true)),
    "event_format"             -> (("StringType",     true)),
    "event_version"            -> (("StringType",     true)),
    "event_fingerprint"        -> (("StringType",     true)),
    "true_tstamp"              -> (("StringType",  true))
  )

  val Atomic = StructType(List(
    StructField("schema_vendor",                 StringType,     true),
    StructField("schema_name",                   StringType,     true),
    StructField("schema_format",                 StringType,     true),
    StructField("schema_version",                StringType,     true),

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
    StructField("tr_total",                 DoubleType,     true),
    StructField("tr_tax",                   DoubleType,     true),
    StructField("tr_shipping",              DoubleType,     true),
    StructField("tr_city",                  StringType,     true),
    StructField("tr_state",                 StringType,     true),
    StructField("tr_country",               StringType,     true),
    StructField("ti_orderid",               StringType,     true),
    StructField("ti_sku",                   StringType,     true),
    StructField("ti_name",                  StringType,     true),
    StructField("ti_category",              StringType,     true),
    StructField("ti_price",                 DoubleType,     true),
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
    StructField("tr_total_base",            DoubleType,     true),
    StructField("tr_tax_base",              DoubleType,     true),
    StructField("tr_shipping_base",         DoubleType,     true),
    StructField("ti_currency",              StringType,     true),
    StructField("ti_price_base",            DoubleType,     true),
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
  ))


  def buildSchemaMap(igluConfig: Json, shreddedTypes: List[LoaderMessage.ShreddedType]): Map[SchemaKey, StructType] =
    shreddedTypes
      .collect { case LoaderMessage.ShreddedType(schemaKey, format) if format == LoaderMessage.Format.PARQUET => schemaKey }
      .traverse { schemaKey =>
        if (schemaKey == Common.AtomicSchema) Validated.validNel(schemaKey -> SparkSchema.Atomic)
        else fetchSchema(igluConfig, schemaKey).map(schema => schemaKey -> schema).leftMap(_ => schemaKey).toValidatedNel
    } match {
      case Validated.Invalid(schemas) => 
        throw new RuntimeException(s"Could not fetch following schema lists to build a Dataframe schema: ${schemas.map(_.toSchemaUri).mkString_("", ",", "")}")
      case Validated.Valid(list) =>
        list.toMap
    }

  def fetchSchema(igluConfig: Json, schemaKey: SchemaKey): Either[FailureDetails.LoaderIgluError, StructType] = {
    def retry[E, A](get: () => Either[E, A], attempt: Int): Either[E, A] =
      get() match {
        case Right(a) => Right(a)
        case Left(_) if attempt < 3 =>
          Thread.sleep(500)
          retry(get, attempt + 1)
        case Left(value) => Left(value)
      }

    val getSchema: () => Either[FailureDetails.LoaderIgluError, StructType] =
      () => Flattening
        .getOrdered[Id](singleton.IgluSingleton.get(igluConfig).resolver, schemaKey.vendor, schemaKey.name, schemaKey.version.model)
        .map(Columnar.getSparkSchema)
        .value

    retry(getSchema, 0)
  }
}
