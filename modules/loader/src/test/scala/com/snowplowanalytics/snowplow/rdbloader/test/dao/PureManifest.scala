package com.snowplowanalytics.snowplow.rdbloader.test.dao

import cats.syntax.all._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.rdbloader.algebras.db.Manifest
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, S3}
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.test.Pure

import java.time.Instant

object PureManifest {
  def interpreter(
    getReply: Iterator[Either[Throwable, Option[Manifest.Entry]]]
  ): Manifest[Pure] = new Manifest[Pure] {
    override def initialize: Pure[Unit] = Pure.sql("manifest initialize")

    override def add(message: LoaderMessage.ManifestItem): Pure[Unit] =
      Pure.sql(s"manifest add ${message.base}")

    override def get(base: Folder): Pure[Option[Manifest.Entry]] =
      getReply.next match {
        case Left(value)  => Pure.fail(value)
        case Right(value) => Pure.sql(s"manifest get ${base}").as(value)
      }

  }

  val ValidMessage = LoaderMessage.ManifestItem(
    S3.Folder.coerce("s3://bucket/folder/"),
    List(
      LoaderMessage.ManifestType(
        SchemaKey("com.acme", "event-a", "jsonschema", SchemaVer.Full(1, 0, 0)),
        "TSV",
        None
      )
    ),
    LoaderMessage.Timestamps(
      Instant.ofEpochMilli(1600342341145L),
      Instant.ofEpochMilli(1600342341145L),
      None,
      None
    ),
    Compression.Gzip,
    LoaderMessage.Processor("test-shredder", Semver(1, 1, 2)),
    None
  )
}
