package com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet

import cats.data.NonEmptyList
import cats.implicits._
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.schemaddl.parquet.FieldValue.DecimalValue
import com.snowplowanalytics.iglu.schemaddl.parquet.Type.DecimalPrecision
import com.snowplowanalytics.iglu.schemaddl.parquet.{CastError, Field, FieldValue, Type}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, FailureDetails, Payload, Processor}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data.ParquetData
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data.ParquetData.FieldWithValue
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields._
import io.circe.Json

import java.time.Instant

object ParquetTransformer {

  def transform(
    event: Event,
    allFields: AllFields,
    processor: Processor
  ): Either[BadRow, Transformed.Parquet] =
    for {
      atomicValues <- extractAtomicFieldValues(allFields.atomic, event)
      nonAtomicValues <- extractNonAtomicFieldValues(allFields.nonAtomicFields, processor, event)
    } yield Transformed.Parquet(ParquetData(atomicValues ::: nonAtomicValues))

  private def extractAtomicFieldValues(atomicFields: AtomicFields, event: Event): Either[BadRow, List[FieldWithValue]] = {

    val atomicValues: List[FieldValue] = List(
      event.app_id.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.platform.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.etl_tstamp.flatMap(castTimestamp).getOrElse(FieldValue.NullValue),
      castTimestamp(event.collector_tstamp).getOrElse(FieldValue.NullValue),
      event.dvce_created_tstamp.flatMap(castTimestamp).getOrElse(FieldValue.NullValue),
      event.event.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      FieldValue.StringValue(event.event_id.toString),
      event.txn_id.map(FieldValue.IntValue).getOrElse(FieldValue.NullValue),
      event.name_tracker.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.v_tracker.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      FieldValue.StringValue(event.v_collector),
      FieldValue.StringValue(event.v_etl),
      event.user_id.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.user_ipaddress.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.user_fingerprint.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.domain_userid.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.domain_sessionidx.map(FieldValue.IntValue).getOrElse(FieldValue.NullValue),
      event.network_userid.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.geo_country.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.geo_region.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.geo_city.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.geo_zipcode.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.geo_latitude.map(FieldValue.DoubleValue).getOrElse(FieldValue.NullValue),
      event.geo_longitude.map(FieldValue.DoubleValue).getOrElse(FieldValue.NullValue),
      event.geo_region_name.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.ip_isp.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.ip_organization.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.ip_domain.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.ip_netspeed.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.page_url.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.page_title.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.page_referrer.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.page_urlscheme.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.page_urlhost.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.page_urlport.map(FieldValue.IntValue).getOrElse(FieldValue.NullValue),
      event.page_urlpath.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.page_urlquery.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.page_urlfragment.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.refr_urlscheme.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.refr_urlhost.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.refr_urlport.map(FieldValue.IntValue).getOrElse(FieldValue.NullValue),
      event.refr_urlpath.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.refr_urlquery.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.refr_urlfragment.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.refr_medium.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.refr_source.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.refr_term.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.mkt_medium.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.mkt_source.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.mkt_term.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.mkt_content.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.mkt_campaign.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.se_category.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.se_action.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.se_label.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.se_property.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.se_value.map(FieldValue.DoubleValue).getOrElse(FieldValue.NullValue),
      event.tr_orderid.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.tr_affiliation.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.tr_total.flatMap(castDecimal).getOrElse(FieldValue.NullValue),
      event.tr_tax.flatMap(castDecimal).getOrElse(FieldValue.NullValue),
      event.tr_shipping.flatMap(castDecimal).getOrElse(FieldValue.NullValue),
      event.tr_city.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.tr_state.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.tr_country.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.ti_orderid.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.ti_sku.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.ti_name.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.ti_category.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.ti_price.flatMap(castDecimal).getOrElse(FieldValue.NullValue),
      event.ti_quantity.map(FieldValue.IntValue).getOrElse(FieldValue.NullValue),
      event.pp_xoffset_min.map(FieldValue.IntValue).getOrElse(FieldValue.NullValue),
      event.pp_xoffset_max.map(FieldValue.IntValue).getOrElse(FieldValue.NullValue),
      event.pp_yoffset_min.map(FieldValue.IntValue).getOrElse(FieldValue.NullValue),
      event.pp_yoffset_max.map(FieldValue.IntValue).getOrElse(FieldValue.NullValue),
      event.useragent.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.br_name.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.br_family.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.br_version.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.br_type.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.br_renderengine.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.br_lang.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.br_features_pdf.map(FieldValue.BooleanValue).getOrElse(FieldValue.NullValue),
      event.br_features_flash.map(FieldValue.BooleanValue).getOrElse(FieldValue.NullValue),
      event.br_features_java.map(FieldValue.BooleanValue).getOrElse(FieldValue.NullValue),
      event.br_features_director.map(FieldValue.BooleanValue).getOrElse(FieldValue.NullValue),
      event.br_features_quicktime.map(FieldValue.BooleanValue).getOrElse(FieldValue.NullValue),
      event.br_features_realplayer.map(FieldValue.BooleanValue).getOrElse(FieldValue.NullValue),
      event.br_features_windowsmedia.map(FieldValue.BooleanValue).getOrElse(FieldValue.NullValue),
      event.br_features_gears.map(FieldValue.BooleanValue).getOrElse(FieldValue.NullValue),
      event.br_features_silverlight.map(FieldValue.BooleanValue).getOrElse(FieldValue.NullValue),
      event.br_cookies.map(FieldValue.BooleanValue).getOrElse(FieldValue.NullValue),
      event.br_colordepth.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.br_viewwidth.map(FieldValue.IntValue).getOrElse(FieldValue.NullValue),
      event.br_viewheight.map(FieldValue.IntValue).getOrElse(FieldValue.NullValue),
      event.os_name.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.os_family.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.os_manufacturer.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.os_timezone.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.dvce_type.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.dvce_ismobile.map(FieldValue.BooleanValue).getOrElse(FieldValue.NullValue),
      event.dvce_screenwidth.map(FieldValue.IntValue).getOrElse(FieldValue.NullValue),
      event.dvce_screenheight.map(FieldValue.IntValue).getOrElse(FieldValue.NullValue),
      event.doc_charset.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.doc_width.map(FieldValue.IntValue).getOrElse(FieldValue.NullValue),
      event.doc_height.map(FieldValue.IntValue).getOrElse(FieldValue.NullValue),
      event.tr_currency.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.tr_total_base.flatMap(castDecimal).getOrElse(FieldValue.NullValue),
      event.tr_tax_base.flatMap(castDecimal).getOrElse(FieldValue.NullValue),
      event.tr_shipping_base.flatMap(castDecimal).getOrElse(FieldValue.NullValue),
      event.ti_currency.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.ti_price_base.flatMap(castDecimal).getOrElse(FieldValue.NullValue),
      event.base_currency.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.geo_timezone.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.mkt_clickid.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.mkt_network.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.etl_tags.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.dvce_sent_tstamp.flatMap(castTimestamp).getOrElse(FieldValue.NullValue),
      event.refr_domain_userid.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.refr_dvce_tstamp.flatMap(castTimestamp).getOrElse(FieldValue.NullValue),
      event.domain_sessionid.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.derived_tstamp.flatMap(castTimestamp).getOrElse(FieldValue.NullValue),
      event.event_vendor.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.event_name.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.event_format.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.event_version.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.event_fingerprint.map(FieldValue.StringValue).getOrElse(FieldValue.NullValue),
      event.true_tstamp.flatMap(castTimestamp).getOrElse(FieldValue.NullValue)
    )

    Right {
      atomicFields.value.zip(atomicValues).map { case (field, value) =>
        FieldWithValue(field, value)
      }
    }
  }

  private def castDecimal(value: Double): Option[FieldValue] = {
    val bigDec = BigDecimal.valueOf(value)
    Either.catchOnly[java.lang.ArithmeticException](bigDec.setScale(2, BigDecimal.RoundingMode.UNNECESSARY)) match {
      case Right(scaled) if bigDec.precision <= Type.DecimalPrecision.toInt(DecimalPrecision.Digits18) =>
        Some(DecimalValue(scaled, DecimalPrecision.Digits18))
      case _ =>
        None
    }
  }

  private def castTimestamp(value: Instant): Option[FieldValue] =
    Either.catchNonFatal(FieldValue.TimestampValue(java.sql.Timestamp.from(value))).toOption

  private def extractNonAtomicFieldValues(
    nonAtomicFields: NonAtomicFields,
    processor: Processor,
    event: Event
  ): Either[BadRow, List[FieldWithValue]] =
    nonAtomicFields.value
      .traverse { typedField =>
        valueFromTypedField(typedField, event)
          .leftMap(castingBadRow(event, processor, typedField.`type`.schemaKey))
      }

  private def valueFromTypedField(fieldWithType: TypedField, event: Event): Either[NonEmptyList[CastError], FieldWithValue] =
    fieldWithType.`type`.snowplowEntity match {
      case LoaderMessage.SnowplowEntity.SelfDescribingEvent =>
        forUnstruct(fieldWithType, event)
      case LoaderMessage.SnowplowEntity.Context =>
        forContexts(fieldWithType, event)
    }

  private def forUnstruct(typedField: TypedField, event: Event): Either[NonEmptyList[CastError], FieldWithValue] =
    event.unstruct_event.data match {
      case Some(SelfDescribingData(schemaKey, unstructData)) if keysMatch(schemaKey, typedField.`type`.schemaKey) =>
        provideValue(typedField.field, unstructData)
      case _ =>
        Right(FieldWithValue(typedField.field, FieldValue.NullValue))
    }

  private def forContexts(typedField: TypedField, event: Event): Either[NonEmptyList[CastError], FieldWithValue] = {
    val allContexts = event.contexts.data ::: event.derived_contexts.data
    val matchingContexts = allContexts
      .filter(context => keysMatch(context.schema, typedField.`type`.schemaKey))

    if (matchingContexts.nonEmpty) {
      val jsonArrayWithContexts = Json.fromValues(matchingContexts.map(_.data).toVector)
      provideValue(typedField.field, jsonArrayWithContexts)
    } else {
      Right(FieldWithValue(typedField.field, FieldValue.NullValue))
    }
  }

  private def provideValue(field: Field, jsonValue: Json): Either[NonEmptyList[CastError], FieldWithValue] =
    FieldValue
      .cast(field)(jsonValue)
      .toEither
      .map(value => FieldWithValue(field, value))

  private def keysMatch(k1: SchemaKey, k2: SchemaKey): Boolean =
    k1.vendor === k2.vendor && k1.name === k2.name && k1.version.model === k2.version.model

  private def castingBadRow(
    event: Event,
    processor: Processor,
    schemaKey: SchemaKey
  )(
    error: NonEmptyList[CastError]
  ): BadRow = {
    val loaderIgluErrors = error.map(castErrorToLoaderIgluError(schemaKey))
    igluBadRow(event, processor, loaderIgluErrors)
  }

  private def castErrorToLoaderIgluError(schemaKey: SchemaKey)(castError: CastError): FailureDetails.LoaderIgluError =
    castError match {
      case CastError.WrongType(v, e) => FailureDetails.LoaderIgluError.WrongType(schemaKey, v, e.toString)
      case CastError.MissingInValue(k, v) => FailureDetails.LoaderIgluError.MissingInValue(schemaKey, k, v)
    }

  private def igluBadRow(
    event: Event,
    processor: Processor,
    errors: NonEmptyList[FailureDetails.LoaderIgluError]
  ): BadRow = {
    val failure = Failure.LoaderIgluErrors(errors)
    val payload = Payload.LoaderPayload(event)
    BadRow.LoaderIgluError(processor, failure, payload)
  }
}
