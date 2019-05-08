package com.snowplowanalytics.snowplow.fs2shredder

import java.nio.file.{ Path, Files }

import cats.effect.Sync
import cats.implicits._

import io.circe.jawn.parse

import com.monovore.decline._
import com.snowplowanalytics.iglu.client.resolver.Resolver

/**
  * Case class representing the configuration for the shred job.
 *
  * @param inFolder Folder where the input events are located
  * @param outFolder Output folder where the shredded events will be stored
  * @param badFolder Output folder where the malformed events will be stored
  * @param igluConfig JSON representing the Iglu configuration
  * @param jsonOnly don't try to produce TSV output
  */
case class ShredderCli(inFolder: Path,
                       outFolder: Path,
                       badFolder: Path,
                       igluConfig: Path,
                       jsonOnly: Boolean) {
}

object ShredderCli {

  val inputFolder = Opts.option[Path]("input-folder",
    "Folder where the input events are located",
    metavar = "<path>")
  val outputFolder = Opts.option[Path]("output-folder",
    "Output folder where the shredded events will be stored",
    metavar = "<path>")
  val badFolder = Opts.option[Path]("bad-folder",
    "Output folder where the malformed events will be stored",
    metavar = "<path>")
  val igluConfig = Opts.option[Path]("iglu-config",
    "Base64-encoded Iglu Client JSON config",
    metavar = "<file>")

  val jsonOnly = Opts.flag("json-only", "Do not produce tabular output").orFalse

  val shredJobConfig = (inputFolder, outputFolder, badFolder, igluConfig, jsonOnly).mapN {
    (input, output, bad, iglu, jsonOnly) => ShredderCli(input, output, bad, iglu, jsonOnly)
  }


  val command = Command(s"fs2-shredder-0.1.0",
    "App to prepare Snowplow enriched data to being loaded into Amazon Redshift warehouse")(shredJobConfig)

  def loadResolver[F[_]: Sync](path: Path): F[Resolver[F]] =
    Sync[F].delay { Files.readString(path) }
      .map { str => parse(str).valueOr(throw _) }
      .flatMap { json => Resolver.parse[F](json) }
      .map { x => x.valueOr(throw _) }


}
